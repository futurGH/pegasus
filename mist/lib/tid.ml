type t = string

let charset = "234567abcdefghijklmnopqrstuvwxyz"

let encode (n : int64) : t =
  let rec encode ~tid n =
    match n with
    | 0L ->
        tid
    | n ->
        encode
          ~tid:(String.make 1 charset.[Int64.to_int (Int64.rem n 32L)] ^ tid)
          (Int64.unsigned_div n 32L)
  in
  encode ~tid:"" n

let decode (s : t) : int64 =
  let rec decode ~(n : int64) (s : string) =
    match s with
    | s when String.length s > 0 ->
        let c = s.[0] in
        let cs = String.sub s 1 (String.length s - 1) in
        decode
          ~n:
            (Int64.add (Int64.mul n 32L)
               (Int64.of_int (String.index charset c)) )
          cs
    | _ ->
        n
  in
  decode ~n:0L s

let now () : t =
  Mtime_clock.now_ns () |> Int64.unsigned_div 1_000_000L |> encode

let of_string (s : string) : t =
  match String.length s with
  | 13
    when Str.string_match
           (Str.regexp
              "/^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$/" )
           s 0 ->
      s
  | _ ->
      raise (Invalid_argument (Format.sprintf "invalid tid: %s" s))

let to_string (s : t) : string = s

let of_yojson = function
  | `String s ->
      Ok (of_string s)
  | _ ->
      Error "expected string tid"

let to_yojson s = `String (to_string s)

let compare = String.compare

let hash = Hashtbl.hash

let equal = ( = )

let pp fmt t = Format.fprintf fmt "%s" t
