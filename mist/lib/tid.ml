type t = string

let charset = "234567abcdefghijklmnopqrstuvwxyz"

let tid_regexp =
  Re.Pcre.regexp "[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}"

let is_valid (s : string) : bool =
  match String.length s with
  | 13 when Re.execp tid_regexp s ->
      true
  | _ ->
      false

let ensure_valid (tid : t) : unit =
  if String.length tid <> 13 then
    raise
      (Invalid_argument
         (Format.sprintf "invalid tid length %d: %s" (String.length tid) tid) )
  else if not (Re.execp tid_regexp tid) then
    raise (Invalid_argument (Format.sprintf "invalid tid format: %s" tid))

let _encode (n : int64) : t =
  let rec _encode ~tid n =
    match n with
    | 0L ->
        tid
    | n ->
        _encode
          ~tid:(String.make 1 charset.[Int64.to_int (Int64.rem n 32L)] ^ tid)
          (Int64.unsigned_div n 32L)
  in
  _encode ~tid:"" n

let _decode (s : t) : int64 =
  let rec _decode ~(n : int64) (s : string) =
    match s with
    | s when String.length s > 0 ->
        let c = s.[0] in
        let cs = String.sub s 1 (String.length s - 1) in
        _decode
          ~n:
            (Int64.add (Int64.mul n 32L)
               (Int64.of_int (String.index charset c)) )
          cs
    | _ ->
        n
  in
  _decode ~n:0L s

let of_timestamp_us (timestamp : int64) ~(clockid : int) : t =
  if timestamp < 0L || timestamp >= Int64.shift_left 1L 53 then
    raise (Invalid_argument "timestamp must be within range [0, 2^53)") ;
  if clockid < 0 || clockid > 1023 then
    raise (Invalid_argument "clockid must be within range [0, 1023]") ;
  let rec pad str len =
    if String.length str >= len then str else pad ("2" ^ str) len
  in
  pad (_encode timestamp) 11 ^ pad (_encode @@ Int64.of_int clockid) 2

let of_timestamp_ms (timestamp : int64) ~(clockid : int) : t =
  of_timestamp_us (Int64.mul timestamp 1000L) ~clockid

let to_timestamp_us (tid : t) : int64 * int =
  ensure_valid tid ;
  let timestamp = _decode (String.sub tid 0 11) in
  let clockid = Int64.to_int @@ _decode (String.sub tid 11 2) in
  (timestamp, clockid)

let to_timestamp_ms (tid : t) : int64 * int =
  ensure_valid tid ;
  let timestamp = _decode (String.sub tid 0 11) in
  let clockid = Int64.to_int @@ _decode (String.sub tid 11 2) in
  (Int64.div timestamp 1000L, clockid)

let now () : t =
  Mtime_clock.now_ns () |> Int64.unsigned_div 1_000L
  |> of_timestamp_us ~clockid:(Random.int_in_range ~min:0 ~max:1023)

let of_string (s : string) : t = ensure_valid s ; s

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
