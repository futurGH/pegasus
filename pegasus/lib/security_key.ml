open Util.Rapper

let max_security_keys_per_user = 5

let look_ahead_window = 100

let resync_window = 1000

let code_digits = 6

let secret_length = 20 (* 160 bits for HMAC-SHA1 *)

module Types = struct
  type security_key =
    { id: int
    ; did: string
    ; name: string
    ; secret: bytes
    ; counter: int
    ; created_at: int
    ; last_used_at: int option
    ; verified_at: int option }
end

open Types

module Queries = struct
  let insert_security_key =
    [%rapper
      execute
        {sql| INSERT INTO security_keys (did, name, secret, counter, created_at)
              VALUES (%string{did}, %string{name}, %Blob{secret}, %int{counter}, %int{created_at})
        |sql}]

  let get_last_insert_id =
    [%rapper get_one {sql| SELECT last_insert_rowid() AS @int{id} |sql}]

  let get_security_keys_by_did =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{did}, @string{name}, @Blob{secret},
                     @int{counter}, @int{created_at}, @int?{last_used_at}, @int?{verified_at}
              FROM security_keys WHERE did = %string{did}
              ORDER BY created_at DESC
        |sql}
        record_out]

  let get_verified_security_keys_by_did =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{did}, @string{name}, @Blob{secret},
                     @int{counter}, @int{created_at}, @int?{last_used_at}, @int?{verified_at}
              FROM security_keys WHERE did = %string{did} AND verified_at IS NOT NULL
              ORDER BY created_at DESC
        |sql}
        record_out]

  let get_security_key_by_id id did =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @string{did}, @string{name}, @Blob{secret},
                     @int{counter}, @int{created_at}, @int?{last_used_at}, @int?{verified_at}
              FROM security_keys WHERE id = %int{id} AND did = %string{did}
        |sql}
        record_out]
      ~id ~did

  let update_counter_and_last_used =
    [%rapper
      execute
        {sql| UPDATE security_keys SET counter = %int{counter}, last_used_at = %int{last_used_at}
              WHERE id = %int{id}
        |sql}]

  let update_counter =
    [%rapper
      execute
        {sql| UPDATE security_keys SET counter = %int{counter}
              WHERE id = %int{id}
        |sql}]

  let verify_security_key =
    [%rapper
      execute
        {sql| UPDATE security_keys SET verified_at = %int{verified_at}, counter = %int{counter}
              WHERE id = %int{id} AND did = %string{did}
        |sql}]

  let delete_security_key =
    [%rapper
      execute
        {sql| DELETE FROM security_keys WHERE id = %int{id} AND did = %string{did}
        |sql}]

  let count_security_keys =
    [%rapper
      get_one
        {sql| SELECT COUNT(*) AS @int{count} FROM security_keys WHERE did = %string{did}
        |sql}]

  let count_verified_security_keys =
    [%rapper
      get_one
        {sql| SELECT COUNT(*) AS @int{count} FROM security_keys
              WHERE did = %string{did} AND verified_at IS NOT NULL
        |sql}]

  let has_security_keys =
    [%rapper
      get_opt
        {sql| SELECT 1 AS @int{has_sk} FROM security_keys
              WHERE did = %string{did} AND verified_at IS NOT NULL LIMIT 1
        |sql}]
end

(* RFC 4226 *)
let hotp ~(secret : bytes) ~(counter : int64) : string =
  (* convert counter to 8-byte big-endian *)
  let counter_bytes = Bytes.create 8 in
  let c = ref counter in
  for i = 7 downto 0 do
    Bytes.set counter_bytes i (Char.chr (Int64.to_int (Int64.logand !c 0xffL))) ;
    c := Int64.shift_right_logical !c 8
  done ;
  let hmac =
    Digestif.SHA1.(
      hmac_bytes ~key:(Bytes.to_string secret) counter_bytes |> to_raw_string )
  in
  (* dynamic truncation *)
  let offset = Char.code hmac.[19] land 0xf in
  let code =
    ((Char.code hmac.[offset] land 0x7f) lsl 24)
    lor ((Char.code hmac.[offset + 1] land 0xff) lsl 16)
    lor ((Char.code hmac.[offset + 2] land 0xff) lsl 8)
    lor (Char.code hmac.[offset + 3] land 0xff)
  in
  let modulo = int_of_float (10. ** float_of_int code_digits) in
  Printf.sprintf "%0*d" code_digits (code mod modulo)

let generate_secret () =
  let () = Mirage_crypto_rng_unix.use_default () in
  Bytes.of_string (Mirage_crypto_rng_unix.getrandom secret_length)

let make_provisioning_uri ~secret ~account ~issuer =
  let secret_b32 =
    Multibase.Base32.encode_exn ~pad:false (Bytes.to_string secret)
  in
  let encoded_account = Uri.pct_encode account in
  let encoded_issuer = Uri.pct_encode issuer in
  Printf.sprintf
    "otpauth://hotp/%s:%s?secret=%s&issuer=%s&algorithm=SHA1&digits=%d&counter=0"
    encoded_issuer encoded_account secret_b32 encoded_issuer code_digits

let verify_code ~secret ~stored_counter ~code =
  let rec check_window offset =
    if offset > look_ahead_window then Error "Code not valid (may need resync)"
    else
      let counter = Int64.of_int (stored_counter + offset) in
      if hotp ~secret ~counter = code then Ok (stored_counter + offset + 1)
        (* update counter past this one *)
      else check_window (offset + 1)
  in
  check_window 0

(* resync requires two consecutive valid codes *)
let resync ~secret ~stored_counter ~code1 ~code2 =
  let rec find_first offset =
    if offset > resync_window then None
    else
      let counter = Int64.of_int (stored_counter + offset) in
      if hotp ~secret ~counter = code1 then Some (stored_counter + offset)
      else find_first (offset + 1)
  in
  match find_first 0 with
  | None ->
      Error "First code not found in resync window"
  | Some counter1 ->
      let counter2 = Int64.of_int (counter1 + 1) in
      if hotp ~secret ~counter:counter2 = code2 then Ok (counter1 + 2)
        (* resync to after both codes *)
      else Error "Second code must immediately follow the first"

let setup_security_key ~did ~name db =
  let secret = generate_secret () in
  let now = Util.Time.now_ms () in
  let%lwt () =
    Util.Sqlite.use_pool db
    @@ Queries.insert_security_key ~did ~name ~secret ~counter:0 ~created_at:now
  in
  let%lwt id = Util.Sqlite.use_pool db @@ Queries.get_last_insert_id () in
  let issuer = "Pegasus PDS (" ^ Env.hostname ^ ")" in
  let uri = make_provisioning_uri ~secret ~account:did ~issuer in
  let secret_b32 =
    Multibase.Base32.encode_exn ~pad:false (Bytes.to_string secret)
  in
  Lwt.return (id, secret_b32, uri)

let verify_setup ~id ~did ~code db =
  match%lwt
    Util.Sqlite.use_pool db @@ Queries.get_security_key_by_id id did
  with
  | None ->
      Lwt.return_error "Security key not found"
  | Some sk -> (
      if Option.is_some sk.verified_at then
        Lwt.return_error "Security key already verified"
      else
        match
          verify_code ~secret:sk.secret ~stored_counter:sk.counter ~code
        with
        | Error msg ->
            Lwt.return_error msg
        | Ok new_counter ->
            let now = Util.Time.now_ms () in
            let%lwt () =
              Util.Sqlite.use_pool db
              @@ Queries.verify_security_key ~id ~did ~verified_at:now
                   ~counter:new_counter
            in
            Lwt.return_ok () )

let verify_login ~did ~code db =
  let%lwt keys =
    Util.Sqlite.use_pool db @@ Queries.get_verified_security_keys_by_did ~did
  in
  let rec try_keys = function
    | [] ->
        Lwt.return_false
    | sk :: rest -> (
      match verify_code ~secret:sk.secret ~stored_counter:sk.counter ~code with
      | Error _ ->
          try_keys rest
      | Ok new_counter ->
          let now = Util.Time.now_ms () in
          let%lwt () =
            Util.Sqlite.use_pool db
            @@ Queries.update_counter_and_last_used ~id:sk.id
                 ~counter:new_counter ~last_used_at:now
          in
          Lwt.return_true )
  in
  try_keys keys

let resync_key ~id ~did ~code1 ~code2 db =
  match%lwt
    Util.Sqlite.use_pool db @@ Queries.get_security_key_by_id id did
  with
  | None ->
      Lwt.return_error "Security key not found"
  | Some sk -> (
      if Option.is_none sk.verified_at then
        Lwt.return_error "Security key not verified yet"
      else
        match
          resync ~secret:sk.secret ~stored_counter:sk.counter ~code1 ~code2
        with
        | Error msg ->
            Lwt.return_error msg
        | Ok new_counter ->
            let%lwt () =
              Util.Sqlite.use_pool db
              @@ Queries.update_counter ~id:sk.id ~counter:new_counter
            in
            Lwt.return_ok () )

let get_keys_for_user ~did db =
  Util.Sqlite.use_pool db @@ Queries.get_security_keys_by_did ~did

let delete_key ~id ~did db =
  let%lwt () =
    Util.Sqlite.use_pool db @@ Queries.delete_security_key ~id ~did
  in
  Lwt.return_true

let has_security_keys ~did db =
  match%lwt Util.Sqlite.use_pool db @@ Queries.has_security_keys ~did with
  | Some _ ->
      Lwt.return_true
  | None ->
      Lwt.return_false

let count_security_keys ~did db =
  Util.Sqlite.use_pool db @@ Queries.count_security_keys ~did

let count_verified_security_keys ~did db =
  Util.Sqlite.use_pool db @@ Queries.count_verified_security_keys ~did
