open Util.Rapper

let secret_length = 20 (* 160 bits for HMAC-SHA1 *)

let time_step = 30 (* 30 second intervals *)

let code_digits = 6

let window_size = 1

module Backup_codes = struct
  let code_count = 10

  let code_length = 8

  module Types = struct
    type backup_code =
      { id: int
      ; did: string
      ; code_hash: string
      ; used_at: int option
      ; created_at: int }
  end

  open Types

  module Queries = struct
    let insert_backup_code =
      [%rapper
        execute
          {sql| INSERT INTO backup_codes (did, code_hash, created_at)
              VALUES (%string{did}, %string{code_hash}, %int{created_at})
        |sql}]

    let get_backup_codes_by_did did =
      [%rapper
        get_many
          {sql| SELECT @int{id}, @string{did}, @string{code_hash}, @int?{used_at}, @int{created_at}
              FROM backup_codes WHERE did = %string{did}
              ORDER BY created_at ASC
        |sql}
          record_out]
        did

    let get_unused_backup_codes_by_did did =
      [%rapper
        get_many
          {sql| SELECT @int{id}, @string{did}, @string{code_hash}, @int?{used_at}, @int{created_at}
              FROM backup_codes WHERE did = %string{did} AND used_at IS NULL
              ORDER BY created_at ASC
        |sql}
          record_out]
        did

    let mark_code_used =
      [%rapper
        execute
          {sql| UPDATE backup_codes SET used_at = %int{used_at}
              WHERE id = %int{id} AND did = %string{did}
        |sql}]

    let delete_backup_codes_by_did =
      [%rapper
        execute {sql| DELETE FROM backup_codes WHERE did = %string{did} |sql}]

    let count_unused_backup_codes did =
      [%rapper
        get_one
          {sql| SELECT COUNT(*) AS @int{count}
              FROM backup_codes WHERE did = %string{did} AND used_at IS NULL
        |sql}]
        did
  end

  let generate_code () =
    let () = Mirage_crypto_rng_unix.use_default () in
    Multibase.Base32.encode_string ~pad:false
    (* 5 bytes = 8 base32 chars *)
    @@ Mirage_crypto_rng_unix.getrandom 5

  let format_code code =
    if String.length code = 8 then
      String.sub code 0 4 ^ "-" ^ String.sub code 4 4
    else code

  let normalize_code code =
    String.concat "" (String.split_on_char '-' code) |> String.uppercase_ascii

  let generate_codes () = List.init code_count (fun _ -> generate_code ())

  let hash_code code = Bcrypt.hash code |> Bcrypt.string_of_hash

  let verify_code_hash code hash =
    try
      let hash_obj = Bcrypt.hash_of_string hash in
      Bcrypt.verify code hash_obj
    with _ -> false

  let store_codes ~did ~codes db =
    let now = Util.now_ms () in
    Lwt_list.iter_s
      (fun code ->
        let code_hash = hash_code code in
        Util.use_pool db
        @@ Queries.insert_backup_code ~did ~code_hash ~created_at:now )
      codes

  let regenerate ~did db =
    let%lwt () = Util.use_pool db @@ Queries.delete_backup_codes_by_did ~did in
    let codes = generate_codes () in
    let%lwt () = store_codes ~did ~codes db in
    Lwt.return (List.map format_code codes)

  let verify_and_consume ~did ~code db =
    let normalized_code = normalize_code code in
    let%lwt codes =
      Util.use_pool db @@ Queries.get_unused_backup_codes_by_did ~did
    in
    let rec check = function
      | [] ->
          Lwt.return_false
      | c :: rest ->
          if verify_code_hash normalized_code c.code_hash then
            let now = Util.now_ms () in
            let%lwt () =
              Util.use_pool db
              @@ Queries.mark_code_used ~id:c.id ~did ~used_at:now
            in
            Lwt.return_true
          else check rest
    in
    check codes

  let get_remaining_count ~did db =
    Util.use_pool db @@ Queries.count_unused_backup_codes ~did

  let has_backup_codes ~did db =
    let%lwt count = get_remaining_count ~did db in
    Lwt.return (count > 0)

  let ensure_codes_exist ~did db =
    let%lwt count = get_remaining_count ~did db in
    if count > 0 then Lwt.return_none
    else
      let%lwt codes = regenerate ~did db in
      Lwt.return_some codes
end

module Queries = struct
  let set_totp_secret =
    [%rapper
      execute
        {sql| UPDATE actors SET totp_secret = %Blob{secret}, totp_verified_at = NULL
              WHERE did = %string{did}
        |sql}]

  let get_totp_secret did =
    [%rapper
      get_opt
        {sql| SELECT @Blob?{totp_secret}, @int?{totp_verified_at}
              FROM actors WHERE did = %string{did}
        |sql}]
      did

  let verify_totp_secret =
    [%rapper
      execute
        {sql| UPDATE actors SET totp_verified_at = %int{verified_at}
              WHERE did = %string{did}
        |sql}]

  let clear_totp_secret =
    [%rapper
      execute
        {sql| UPDATE actors SET totp_secret = NULL, totp_verified_at = NULL
              WHERE did = %string{did}
        |sql}]

  let is_totp_enabled did =
    [%rapper
      get_opt
        {sql| SELECT 1 AS @int{enabled}
              FROM actors WHERE did = %string{did} AND totp_verified_at IS NOT NULL
        |sql}]
      did
end

let generate_secret () =
  let () = Mirage_crypto_rng_unix.use_default () in
  Bytes.of_string (Mirage_crypto_rng_unix.getrandom secret_length)

let make_provisioning_uri ~secret ~did ~issuer =
  let secret_b32 =
    Multibase.Base32.encode_exn ~pad:false (Bytes.to_string secret)
  in
  let encoded_did = Uri.pct_encode did in
  let encoded_issuer = Uri.pct_encode issuer in
  Printf.sprintf
    "otpauth://totp/%s:%s?secret=%s&issuer=%s&algorithm=SHA1&digits=%d&period=%d"
    encoded_issuer encoded_did secret_b32 encoded_issuer code_digits time_step

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

let generate_code ~secret =
  let counter =
    Int64.div (Int64.of_float (Unix.gettimeofday ())) (Int64.of_int time_step)
  in
  hotp ~secret ~counter

let verify_code ~secret ~code =
  let current_counter =
    Int64.div (Int64.of_float (Unix.gettimeofday ())) (Int64.of_int time_step)
  in
  let rec check offset =
    if offset > window_size then false
    else
      let counter_plus = Int64.add current_counter (Int64.of_int offset) in
      let counter_minus = Int64.sub current_counter (Int64.of_int offset) in
      if hotp ~secret ~counter:counter_plus = code then true
      else if offset > 0 && hotp ~secret ~counter:counter_minus = code then true
      else check (offset + 1)
  in
  check 0

let create_secret ~did ~secret db =
  Util.use_pool db @@ Queries.set_totp_secret ~did ~secret

let get_secret ~did db =
  match%lwt Util.use_pool db @@ Queries.get_totp_secret ~did with
  | Some (Some secret, verified_at) ->
      Lwt.return_some (secret, verified_at)
  | _ ->
      Lwt.return_none

let verify_and_enable ~did ~code db =
  match%lwt get_secret ~did db with
  | None ->
      Lwt.return_error "No TOTP setup in progress"
  | Some (_, Some _) ->
      Lwt.return_error "TOTP is already enabled"
  | Some (secret, None) ->
      if verify_code ~secret ~code then
        let now = Util.now_ms () in
        let%lwt () =
          Util.use_pool db @@ Queries.verify_totp_secret ~did ~verified_at:now
        in
        Lwt.return_ok ()
      else Lwt.return_error "Invalid verification code"

let disable ~did db = Util.use_pool db @@ Queries.clear_totp_secret ~did

let is_enabled ~did db =
  match%lwt Util.use_pool db @@ Queries.is_totp_enabled ~did with
  | Some _ ->
      Lwt.return_true
  | None ->
      Lwt.return_false

let verify_login_code ~did ~code db =
  match%lwt get_secret ~did db with
  | Some (secret, Some _) ->
      Lwt.return (verify_code ~secret ~code)
  | _ ->
      Lwt.return_false
