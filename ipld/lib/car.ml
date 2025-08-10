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
      if counter >= l || shift > 49 then failwith "could not decode varint"
      else
        let b = Bytes.get_uint8 buf counter in
        let new_res =
          if shift < 28 then res + ((b land rest) lsl shift)
          else res + (b land rest * (1 lsl shift))
        in
        let new_counter = counter + 1 in
        let new_shift = shift + 7 in
        if b >= msb then aux new_res new_shift new_counter
        else (new_res, new_counter)
    in
    let result, final_counter = aux 0 0 0 in
    (result, final_counter)
end

(* converts a series of mst blocks into a car stream *)
let blocks_to_stream (root : Cid.t option) (blocks : (Cid.t * bytes) Lwt_seq.t)
    : bytes Lwt_seq.t =
  let header =
    Dag_cbor.encode
      (`Map
         (Dag_cbor.StringMap.of_list
            [ ("version", `Integer 1L)
            ; ( "roots"
              , `Array
                  (match root with None -> [||] | Some root -> [|`Link root|])
              ) ] ) )
  in
  let seq = Lwt_seq.of_list [Varint.encode (Bytes.length header); header] in
  Lwt_seq.append seq
    (Lwt_seq.flat_map
       (fun (cid, block) ->
         Lwt_seq.of_list
           [ Varint.encode
               ((cid |> Cid.to_bytes |> Bytes.length) + Bytes.length block)
           ; cid.bytes
           ; block ] )
       blocks )

(* converts a series of mst blocks into a car file *)
let blocks_to_car (root : Cid.t option) (blocks : (Cid.t * bytes) Lwt_seq.t) :
    bytes Lwt.t =
  let stream = blocks_to_stream root blocks in
  let buf = Buffer.create 1024 in
  let%lwt () = Lwt_seq.iter (Buffer.add_bytes buf) stream in
  Lwt.return (Buffer.to_bytes buf)

(* reads a car stream into a serialized mst
   returns (roots, blocks) *)
let read_car_stream (stream : bytes Lwt_seq.t) :
    (Cid.t list * (Cid.t * bytes) Lwt_seq.t) Lwt.t =
  let buf = Buffer.create 1024 in
  let%lwt () = Lwt_seq.iter (Buffer.add_bytes buf) stream in
  let bytes = Buffer.to_bytes buf in
  let bytes_len = Bytes.length bytes in
  let pos = ref 0 in
  let read_varint () =
    if !pos >= bytes_len then None
    else
      let n, used = Varint.decode (Bytes.sub bytes !pos (bytes_len - !pos)) in
      pos := !pos + used ;
      Some n
  in
  let read_bytes n =
    if !pos + n > bytes_len then failwith "unexpected end of car stream"
    else
      let b = Bytes.sub bytes !pos n in
      pos := !pos + n ;
      b
  in
  let header_size =
    match read_varint () with
    | None ->
        failwith "could not parse car header"
    | Some n ->
        n
  in
  let header_bytes = read_bytes header_size in
  let header = Dag_cbor.decode header_bytes in
  let roots =
    match header with
    | `Map m -> (
        let roots_v =
          try Some (Dag_cbor.StringMap.find "roots" m) with Not_found -> None
        in
        match roots_v with
        | Some (`Array arr) ->
            Array.fold_right
              (fun v acc -> match v with `Link cid -> cid :: acc | _ -> acc)
              arr []
        | _ ->
            [] )
    | _ ->
        []
  in
  let rec read_blocks acc =
    if !pos >= bytes_len then List.rev acc
    else
      match read_varint () with
      | None ->
          List.rev acc
      | Some block_size ->
          if block_size <= 0 then read_blocks acc
          else
            let block_bytes = read_bytes block_size in
            let cid, remainder = Cid.decode_first block_bytes in
            read_blocks ((cid, remainder) :: acc)
  in
  let blocks_list = read_blocks [] in
  let blocks_seq = Lwt_seq.of_list blocks_list in
  Lwt.return (roots, blocks_seq)
