(* varint encoding/decoding; ripped from https://github.com/chrisdickinson/varint *)
module Varint = struct
  let bytes = ref 0

  let msb = 0x80

  let rest = 0x7F

  let msball = lnot rest

  let int_threshold = 1 lsl 31

  let encode (n0 : int) : bytes =
    if n0 < 0 then (
      bytes := 0 ;
      failwith "negative numbers not supported" ) ;
    let num = ref n0 in
    let buf = Buffer.create 10 in
    let offset = ref 0 in
    while !num >= int_threshold do
      let byte = !num land 0xFF lor msb in
      Buffer.add_char buf (Char.chr (byte land 0xFF)) ;
      incr offset ;
      num := !num / 128
    done ;
    while !num land msball <> 0 do
      let byte = !num land 0xFF lor msb in
      Buffer.add_char buf (Char.chr (byte land 0xFF)) ;
      incr offset ;
      num := !num lsr 7
    done ;
    let last = !num in
    Buffer.add_char buf (Char.chr (last land 0xFF)) ;
    incr offset ;
    bytes := !offset ;
    Bytes.unsafe_of_string (Buffer.contents buf)

  let decode buf =
    let l = Bytes.length buf in
    let rec aux res shift counter =
      if counter >= l || shift >= 63 then failwith "could not decode varint"
      else
        let b = Bytes.get_uint8 buf counter in
        let payload = Int64.of_int (b land rest) in
        let max_payload =
          Int64.shift_right_logical (Int64.of_int max_int) shift
        in
        if payload > max_payload then failwith "varint exceeds platform integer" ;
        let new_res = Int64.logor res (Int64.shift_left payload shift) in
        let new_counter = counter + 1 in
        let new_shift = shift + 7 in
        if b >= msb then aux new_res new_shift new_counter
        else (Int64.to_int new_res, new_counter)
    in
    let result, final_counter = aux 0L 0 0 in
    (result, final_counter)
end

type block = Cid.t * bytes

type block_stream = block Lwt_seq.t

type stream = bytes Lwt_seq.t

(* converts a series of blocks into a car stream *)
let blocks_to_stream (root : Cid.t) (blocks : block_stream) : stream =
  let header =
    Dag_cbor.encode
      (`Map
         (Dag_cbor.String_map.of_list
            [("version", `Integer 1L); ("roots", `Array [|`Link root|])] ) )
  in
  let seq = Lwt_seq.of_list [Varint.encode (Bytes.length header); header] in
  Lwt_seq.append seq
    (Lwt_seq.flat_map
       (fun ((cid, block) : Cid.t * bytes) ->
         Lwt_seq.of_list
           [ Varint.encode (Bytes.length cid.bytes + Bytes.length block)
           ; cid.bytes
           ; block ] )
       blocks )

(* collects a stream into a car file *)
let collect_stream (stream : stream) : bytes Lwt.t =
  let buf = Buffer.create 1024 in
  let%lwt () = Lwt_seq.iter (Buffer.add_bytes buf) stream in
  Lwt.return (Buffer.to_bytes buf)

(* converts a series of blocks into a car file *)
let blocks_to_car (root : Cid.t) (blocks : block_stream) : bytes Lwt.t =
  blocks_to_stream root blocks |> collect_stream

(* reads a car stream into a series of blocks
   returns (roots, blocks) *)
let read_car_stream (stream : stream) : (Cid.t list * block_stream) Lwt.t =
  let open Lwt.Infix in
  let max_section_size = 64 * 1024 * 1024 in
  let source = ref stream in
  let buf = ref Bytes.empty in
  let pos = ref 0 in
  let len = ref 0 in
  let eof = ref false in
  let rec refill () =
    if !pos < !len || !eof then Lwt.return_unit
    else
      !source ()
      >>= function
      | Lwt_seq.Nil ->
          eof := true ;
          buf := Bytes.empty ;
          pos := 0 ;
          len := 0 ;
          Lwt.return_unit
      | Lwt_seq.Cons (chunk, rest) ->
          source := rest ;
          buf := chunk ;
          pos := 0 ;
          len := Bytes.length chunk ;
          if !len = 0 then refill () else Lwt.return_unit
  in
  let read_byte () =
    refill ()
    >>= fun () ->
    if !pos < !len then (
      let b = Bytes.get_uint8 !buf !pos in
      pos := !pos + 1 ;
      Lwt.return_some b )
    else Lwt.return_none
  in
  let read_exact n =
    if n < 0 || n > max_section_size then
      Lwt.fail_with
        (Printf.sprintf "invalid CAR section size %d (maximum %d)" n
           max_section_size )
    else
      let out = Bytes.create n in
      let rec loop offset remaining =
        if remaining = 0 then Lwt.return out
        else
          refill ()
          >>= fun () ->
          if !pos >= !len && !eof then
            Lwt.fail_with "unexpected end of car stream"
          else
            let avail = !len - !pos in
            let take = if avail < remaining then avail else remaining in
            if take = 0 then loop offset remaining
            else (
              Bytes.blit !buf !pos out offset take ;
              pos := !pos + take ;
              loop (offset + take) (remaining - take) )
      in
      loop 0 n
  in
  let read_varint_stream () =
    let rec aux res shift =
      if shift >= 63 then Lwt.fail_with "could not decode varint"
      else
        read_byte ()
        >>= function
        | None ->
            if shift = 0 then Lwt.return_none
            else Lwt.fail_with "could not decode varint"
        | Some b ->
            let payload = Int64.of_int (b land Varint.rest) in
            let max_payload =
              Int64.shift_right_logical (Int64.of_int max_int) shift
            in
            if payload > max_payload then
              Lwt.fail_with "CAR varint exceeds platform integer"
            else
              let v = Int64.logor res (Int64.shift_left payload shift) in
              if b land Varint.msb <> 0 then aux v (shift + 7)
              else
                let n = Int64.to_int v in
                if n > max_section_size then
                  Lwt.fail_with
                    (Printf.sprintf "CAR section exceeds %d bytes"
                       max_section_size )
                else Lwt.return_some n
    in
    aux 0L 0
  in
  let%lwt header_size_opt = read_varint_stream () in
  let header_size =
    match header_size_opt with
    | None ->
        failwith "could not parse car header"
    | Some n ->
        n
  in
  let%lwt header_bytes = read_exact header_size in
  let header = Dag_cbor.decode header_bytes in
  let roots =
    match header with
    | `Map m -> (
        let version =
          try Some (Dag_cbor.String_map.find "version" m)
          with Not_found -> None
        in
        if version <> Some (`Integer 1L) then
          failwith "CAR header must declare version 1" ;
        let roots_v =
          try Some (Dag_cbor.String_map.find "roots" m) with Not_found -> None
        in
        match roots_v with
        | Some (`Array arr) ->
            Array.to_list arr
            |> List.map (function
              | `Link cid ->
                  cid
              | _ ->
                  failwith "CAR header contains a non-CID root" )
        | _ ->
            failwith "CAR header is missing roots" )
    | _ ->
        failwith "CAR header must be a map"
  in
  let next () =
    read_varint_stream ()
    >>= function
    | None ->
        Lwt.return_none
    | Some block_size ->
        if block_size <= 0 then Lwt.fail_with "CAR block section is empty"
        else
          read_exact block_size
          >>= fun block_bytes ->
          let cid, remainder = Cid.decode_first block_bytes in
          Lwt.return_some (cid, remainder)
  in
  let blocks : (Cid.t * bytes) Lwt_seq.t =
    Lwt_seq.unfold_lwt
      (fun () -> next () >|= function None -> None | Some x -> Some (x, ()))
      ()
  in
  Lwt.return (roots, blocks)
