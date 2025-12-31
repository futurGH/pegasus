open Util.Rapper

let challenge_expiry_ms = 5 * 60 * 1000

module Types = struct
  type passkey =
    { id: int
    ; did: string
    ; credential_id: string
    ; public_key: bytes
    ; sign_count: int
    ; name: string
    ; created_at: int
    ; last_used_at: int option }

  type challenge =
    { challenge: string
    ; did: string option
    ; challenge_type: string
    ; expires_at: int
    ; created_at: int }

  type passkey_display =
    { id: int
    ; name: string
    ; created_at: int
    ; last_used_at: int option [@default None] }
  [@@deriving yojson {strict= false}]
end

open Types

module Queries = struct
  let insert_passkey =
    [%rapper
      execute
        {sql| INSERT INTO passkeys (did, credential_id, public_key, sign_count, name, created_at)
              VALUES (%string{did}, %string{credential_id}, %Blob{public_key}, %int{sign_count}, %string{name}, %int{created_at})
        |sql}]

  let get_passkeys_by_did did =
    [%rapper
      get_many
        {sql| SELECT @int{id}, @string{did}, @string{credential_id}, @Blob{public_key},
                     @int{sign_count}, @string{name}, @int{created_at}, @int?{last_used_at}
              FROM passkeys WHERE did = %string{did}
              ORDER BY created_at DESC
        |sql}
        record_out]
      did

  let get_passkey_by_credential_id credential_id =
    [%rapper
      get_opt
        {sql| SELECT @int{id}, @string{did}, @string{credential_id}, @Blob{public_key},
                     @int{sign_count}, @string{name}, @int{created_at}, @int?{last_used_at}
              FROM passkeys WHERE credential_id = %string{credential_id}
        |sql}
        record_out]
      credential_id

  let update_passkey_sign_count =
    [%rapper
      execute
        {sql| UPDATE passkeys SET sign_count = %int{sign_count}, last_used_at = %int{last_used_at}
              WHERE credential_id = %string{credential_id}
        |sql}]

  let delete_passkey =
    [%rapper
      execute
        {sql| DELETE FROM passkeys WHERE id = %int{id} AND did = %string{did}
        |sql}]

  let rename_passkey =
    [%rapper
      execute
        {sql| UPDATE passkeys SET name = %string{name} WHERE id = %int{id} AND did = %string{did}
        |sql}]

  let insert_challenge =
    [%rapper
      execute
        {sql| INSERT INTO passkey_challenges (challenge, did, challenge_type, expires_at, created_at)
              VALUES (%string{challenge}, %string?{did}, %string{challenge_type}, %int{expires_at}, %int{created_at})
        |sql}]

  let get_challenge challenge now =
    [%rapper
      get_opt
        {sql| SELECT @string{challenge}, @string?{did}, @string{challenge_type},
                     @int{expires_at}, @int{created_at}
              FROM passkey_challenges
              WHERE challenge = %string{challenge} AND expires_at > %int{now}
        |sql}
        record_out]
      ~challenge ~now

  let delete_challenge =
    [%rapper
      execute
        {sql| DELETE FROM passkey_challenges WHERE challenge = %string{challenge}
        |sql}]
end

let webauthn_instance : Webauthn.t option ref = ref None

let webauthn () =
  match !webauthn_instance with
  | Some t ->
      t
  | None -> (
    match Webauthn.create Env.host_endpoint with
    | Ok t ->
        webauthn_instance := Some t ;
        t
    | Error msg ->
        failwith ("Failed to initialize WebAuthn: " ^ msg) )

let serialize_pubkey (pk : Mirage_crypto_ec.P256.Dsa.pub) : bytes =
  Bytes.of_string (Mirage_crypto_ec.P256.Dsa.pub_to_octets pk)

let deserialize_pubkey (b : bytes) : Mirage_crypto_ec.P256.Dsa.pub option =
  Mirage_crypto_ec.P256.Dsa.pub_of_octets (Bytes.to_string b)
  |> Result.to_option

let create_challenge ?did ~challenge_type db =
  let _challenge_obj, challenge_b64 = Webauthn.generate_challenge () in
  let now = Util.now_ms () in
  let expires_at = now + challenge_expiry_ms in
  let challenge_type_str =
    match challenge_type with
    | `Register ->
        "register"
    | `Authenticate ->
        "authenticate"
  in
  let%lwt () =
    Util.use_pool db
    @@ Queries.insert_challenge ~challenge:challenge_b64 ~did
         ~challenge_type:challenge_type_str ~expires_at ~created_at:now
  in
  Lwt.return challenge_b64

let verify_challenge ~challenge ~challenge_type db =
  let now = Util.now_ms () in
  let expected_type =
    match challenge_type with
    | `Register ->
        "register"
    | `Authenticate ->
        "authenticate"
  in
  match%lwt Util.use_pool db @@ Queries.get_challenge challenge now with
  | Some c when c.challenge_type = expected_type ->
      Lwt.return_some c
  | _ ->
      Lwt.return_none

let delete_challenge ~challenge db =
  Util.use_pool db @@ Queries.delete_challenge ~challenge

let store_credential ~did ~credential_id ~public_key ~name db =
  let now = Util.now_ms () in
  Util.use_pool db
  @@ Queries.insert_passkey ~did ~credential_id ~public_key ~sign_count:0 ~name
       ~created_at:now

let get_credentials_for_user ~did db =
  Util.use_pool db @@ Queries.get_passkeys_by_did ~did

let get_credential_by_id ~credential_id db =
  Util.use_pool db @@ Queries.get_passkey_by_credential_id ~credential_id

let update_sign_count ~credential_id ~sign_count db =
  let now = Util.now_ms () in
  Util.use_pool db
  @@ Queries.update_passkey_sign_count ~credential_id ~sign_count
       ~last_used_at:now

let delete_credential ~id ~did db =
  let%lwt () = Util.use_pool db @@ Queries.delete_passkey ~id ~did in
  Lwt.return_true

let rename_credential ~id ~did ~name db =
  let%lwt () = Util.use_pool db @@ Queries.rename_passkey ~id ~did ~name in
  Lwt.return_true

let generate_registration_options ~did ~email ~existing_credentials db =
  let%lwt challenge = create_challenge ~did ~challenge_type:`Register db in
  let exclude_credentials =
    List.map
      (fun (pk : passkey) ->
        `Assoc
          [ ("id", `String pk.credential_id)
          ; ("type", `String "public-key")
          ; ("transports", `List [`String "internal"; `String "hybrid"]) ] )
      existing_credentials
  in
  let user_id =
    Base64.(encode_string ~alphabet:uri_safe_alphabet ~pad:false did)
  in
  Lwt.return
  @@ `Assoc
       [ ("challenge", `String challenge)
       ; ( "rp"
         , `Assoc [("name", `String "Pegasus PDS"); ("id", `String Env.hostname)]
         )
       ; ( "user"
         , `Assoc
             [ ("id", `String user_id)
             ; ("name", `String email)
             ; ("displayName", `String email) ] )
       ; ( "pubKeyCredParams"
         , `List [`Assoc [("alg", `Int (-7)); ("type", `String "public-key")]]
         )
       ; ("timeout", `Int 300000)
       ; ("attestation", `String "none")
       ; ("excludeCredentials", `List exclude_credentials)
       ; ( "authenticatorSelection"
         , `Assoc
             [ ("residentKey", `String "preferred")
             ; ("userVerification", `String "preferred") ] ) ]

let verify_registration ~challenge ~response db =
  match%lwt verify_challenge ~challenge ~challenge_type:`Register db with
  | None ->
      Lwt.return_error "Invalid or expired challenge"
  | Some _ -> (
      let%lwt () = delete_challenge ~challenge db in
      let credential_json = Yojson.Safe.from_string response in
      let credential_id =
        match credential_json with
        | `Assoc fields -> (
          match List.assoc_opt "id" fields with
          | Some (`String id) ->
              id
          | _ ->
              "" )
        | _ ->
            ""
      in
      let inner_response =
        match credential_json with
        | `Assoc fields -> (
          match List.assoc_opt "response" fields with
          | Some (`Assoc r) -> (
            match
              (* need to extract these fields only, extra fields will cause
                 register_response_of_string to error *)
              ( List.assoc_opt "attestationObject" r
              , List.assoc_opt "clientDataJSON" r )
            with
            | Some ao, Some cd ->
                Yojson.Safe.to_string
                  (`Assoc [("attestationObject", ao); ("clientDataJSON", cd)])
            | _ ->
                "" )
          | _ ->
              "" )
        | _ ->
            ""
      in
      if String.length credential_id = 0 || String.length inner_response = 0
      then Lwt.return_error "invalid credential format"
      else
        match Webauthn.register_response_of_string inner_response with
        | Error e ->
            let err = Format.asprintf "%a" Webauthn.pp_error e in
            Lwt.return_error ("invalid registration response: " ^ err)
        | Ok reg_response -> (
          match Webauthn.register (webauthn ()) reg_response with
          | Error e ->
              let err = Format.asprintf "%a" Webauthn.pp_error e in
              Lwt.return_error ("registration verification failed: " ^ err)
          | Ok (_returned_challenge, registration) ->
              let public_key =
                serialize_pubkey
                  registration.attested_credential_data.public_key
              in
              Lwt.return_ok (credential_id, public_key) ) )

let generate_authentication_options ?did:_did db =
  let%lwt challenge = create_challenge ~challenge_type:`Authenticate db in
  (* for conditional UI, we use empty allowCredentials *)
  Lwt.return
  @@ `Assoc
       [ ("challenge", `String challenge)
       ; ("timeout", `Int 300000)
       ; ("rpId", `String Env.hostname)
       ; ("userVerification", `String "preferred")
       ; ("allowCredentials", `List []) ]

let verify_authentication ~challenge ~response db =
  match%lwt verify_challenge ~challenge ~challenge_type:`Authenticate db with
  | None ->
      Lwt.return_error "invalid or expired challenge"
  | Some _ -> (
      let%lwt () = delete_challenge ~challenge db in
      let credential_json = Yojson.Safe.from_string response in
      let credential_id =
        match credential_json with
        | `Assoc fields -> (
          match List.assoc_opt "id" fields with
          | Some (`String id) ->
              id
          | _ ->
              "" )
        | _ ->
            ""
      in
      let inner_response =
        match credential_json with
        | `Assoc fields -> (
          match List.assoc_opt "response" fields with
          | Some (`Assoc r) -> (
            match
              (* need to extract these fields only, extra fields will cause
                 register_response_of_string to error *)
              ( List.assoc_opt "authenticatorData" r
              , List.assoc_opt "clientDataJSON" r
              , List.assoc_opt "signature" r
              , List.assoc_opt "userHandle" r )
            with
            | Some ad, Some cd, Some sgn, uh ->
                Yojson.Safe.to_string
                  (`Assoc
                     ( ( match uh with
                         | Some uh ->
                             [("userHandle", uh)]
                         | None ->
                             [] )
                     @ [ ("authenticatorData", ad)
                       ; ("clientDataJSON", cd)
                       ; ("signature", sgn) ] ) )
            | _ ->
                "" )
          | _ ->
              "" )
        | _ ->
            ""
      in
      if String.length credential_id = 0 || String.length inner_response = 0
      then Lwt.return_error "invalid credential format"
      else
        match Webauthn.authenticate_response_of_string inner_response with
        | Error _ ->
            Lwt.return_error "invalid authentication response"
        | Ok auth_response -> (
          match%lwt get_credential_by_id ~credential_id db with
          | None ->
              Lwt.return_error "unknown credential"
          | Some passkey -> (
            match deserialize_pubkey passkey.public_key with
            | None ->
                Lwt.return_error "invalid stored public key"
            | Some pubkey -> (
              match
                Webauthn.authenticate (webauthn ()) pubkey auth_response
              with
              | Error _ ->
                  Lwt.return_error "authentication verification failed"
              | Ok (_returned_challenge, auth) ->
                  let sign_count = Int32.to_int auth.sign_count in
                  let%lwt () =
                    update_sign_count ~credential_id ~sign_count db
                  in
                  Lwt.return_ok passkey.did ) ) ) )
