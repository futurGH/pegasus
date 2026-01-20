open Cohttp
open Cohttp_lwt
open Cohttp_lwt_unix
open Util.Types

let default_endpoint = "https://plc.directory"

type t = {did: string; rotation_key: Kleidos.key; endpoint: string}

type service = {type': string [@key "type"]; endpoint: string}
[@@deriving yojson {strict= false}]

type service_map = (string * service) list [@@deriving yojson {strict= false}]

let service_map_to_yojson map =
  `Assoc (List.map (fun (k, v) -> (k, service_to_yojson v)) map)

let service_map_of_yojson = function
  | `Assoc l ->
      Ok
        (List.map
           (fun (k, v) ->
             ( k
             , match service_of_yojson v with
               | Ok s ->
                   s
               | Error _ ->
                   Yojson.json_error ("invalid service " ^ k) ) )
           l )
  | _ ->
      Error "invalid service map"

type credentials =
  { rotation_keys: string list [@key "rotationKeys"]
  ; verification_methods: string_map [@key "verificationMethods"]
  ; also_known_as: string list [@key "alsoKnownAs"]
  ; services: service_map }
[@@deriving yojson {strict= false}]

type unsigned_operation_op =
  { type': string [@key "type"]
  ; rotation_keys: string list [@key "rotationKeys"]
  ; verification_methods: string_map [@key "verificationMethods"]
  ; also_known_as: string list [@key "alsoKnownAs"]
  ; services: service_map
  ; prev: string_or_null }
[@@deriving yojson {strict= false}]

type unsigned_tombstone_op = {type': string [@key "type"]; prev: string}
[@@deriving yojson {strict= false}]

type unsigned_operation =
  | Operation of unsigned_operation_op
  | Tombstone of unsigned_tombstone_op

let unsigned_operation_to_yojson = function
  | Operation op ->
      unsigned_operation_op_to_yojson op
  | Tombstone op ->
      unsigned_tombstone_op_to_yojson op

let unsigned_operation_of_yojson (json : Yojson.Safe.t) =
  match Yojson.Safe.Util.(json |> member "type" |> to_string) with
  | "plc_operation" ->
      Result.map (fun op -> Operation op)
      @@ unsigned_operation_op_of_yojson json
  | "plc_tombstone" ->
      Result.map (fun op -> Tombstone op)
      @@ unsigned_tombstone_op_of_yojson json
  | typ ->
      Error ("invalid operation type " ^ typ)

type signed_operation_op =
  { type': string [@key "type"]
  ; rotation_keys: string list [@key "rotationKeys"]
  ; verification_methods: string_map [@key "verificationMethods"]
  ; also_known_as: string list [@key "alsoKnownAs"]
  ; services: service_map
  ; prev: string_or_null
  ; signature: string [@key "sig"] }
[@@deriving yojson {strict= false}]

type signed_tombstone_op =
  {type': string [@key "type"]; prev: string; signature: string [@key "sig"]}
[@@deriving yojson {strict= false}]

type signed_operation =
  | Operation of signed_operation_op
  | Tombstone of signed_tombstone_op

let signed_operation_to_yojson = function
  | Operation op ->
      signed_operation_op_to_yojson op
  | Tombstone op ->
      signed_tombstone_op_to_yojson op

let signed_operation_of_yojson (json : Yojson.Safe.t) =
  match Yojson.Safe.Util.(json |> member "type" |> to_string) with
  | "plc_operation" ->
      Result.map (fun op -> Operation op) (signed_operation_op_of_yojson json)
  | "plc_tombstone" ->
      Result.map (fun op -> Tombstone op) (signed_tombstone_op_of_yojson json)
  | typ ->
      Error ("unexpected operation type " ^ typ)

type audit_log_operation =
  { signature: string [@key "sig"]
  ; prev: string_or_null
  ; type': string [@key "type"]
  ; services: service_map
  ; also_known_as: string list [@key "alsoKnownAs"]
  ; rotation_keys: string list [@key "rotationKeys"]
  ; verification_methods: string_map [@key "verificationMethods"] }
[@@deriving yojson {strict= false}]

type audit_log_entry =
  { did: string
  ; operation: audit_log_operation
  ; cid: string
  ; nullified: bool
  ; created_at: string [@key "createdAt"] }
[@@deriving yojson {strict= false}]

type audit_log = audit_log_entry list [@@deriving yojson {strict= false}]

let sign_operation (key : Kleidos.key) operation : signed_operation =
  let cbor = unsigned_operation_to_yojson operation |> Dag_cbor.encode_yojson in
  let sig_bytes = Kleidos.sign ~privkey:key ~msg:cbor in
  let sig_str =
    Base64.encode_exn ~pad:false ~alphabet:Base64.uri_safe_alphabet
      (Bytes.to_string sig_bytes)
  in
  match operation with
  | Operation
      {type'; rotation_keys; verification_methods; also_known_as; services; prev}
    ->
      assert (
        Util.sig_matches_some_did_key ~did_keys:rotation_keys
          ~signature:sig_bytes ~msg:cbor ) ;
      Operation
        { type'
        ; rotation_keys
        ; verification_methods
        ; also_known_as
        ; services
        ; prev
        ; signature= sig_str }
  | Tombstone {type'; prev} ->
      Tombstone {type'; prev; signature= sig_str}

let submit_operation ?(endpoint = default_endpoint) did operation :
    (unit, int * string) Lwt_result.t =
  let endpoint = Uri.(with_path (of_string endpoint) did) in
  let headers = Header.of_list [("Content-Type", "application/json")] in
  let body =
    operation |> signed_operation_to_yojson |> Yojson.Safe.to_string
    |> Body.of_string
  in
  let%lwt res, body = Client.post ~headers ~body endpoint in
  match res.status with
  | `OK ->
      let%lwt () = Body.drain_body body in
      Lwt.return_ok ()
  | _ ->
      let%lwt body_str = Body.to_string body in
      Lwt.return_error (Http.Status.to_int res.status, body_str)

let validate_operation ~handle ?signing_key (op : signed_operation) =
  let pds_pubkey =
    Env.rotation_key |> Kleidos.derive_pubkey |> Kleidos.pubkey_to_did_key
  in
  match op with
  | Operation op -> (
      if not (List.mem pds_pubkey op.rotation_keys) then
        Error "rotation keys must include the PDS public key"
      else
        match List.assoc_opt "atproto_pds" op.services with
        | Some {type'; endpoint}
          when type' <> "AtprotoPersonalDataServer"
               || endpoint <> Env.host_endpoint ->
            Error "invalid atproto_pds service"
        | _ ->
            let actor_pubkey =
              signing_key
              |> Option.map (fun sk ->
                  sk |> Kleidos.parse_multikey_str |> Kleidos.derive_pubkey
                  |> Kleidos.pubkey_to_did_key )
            in
            if
              actor_pubkey <> None
              && List.assoc_opt "atproto" op.verification_methods
                 <> actor_pubkey
            then Error "incorrect atproto signing key"
            else if List.hd op.also_known_as <> "at://" ^ handle then
              Error "incorrect handle"
            else Ok () )
  | Tombstone _ ->
      Ok ()

let did_of_operation operation : string =
  let cbor = signed_operation_to_yojson operation |> Dag_cbor.encode_yojson in
  let digest = Digestif.SHA256.(cbor |> digest_bytes |> to_raw_string) in
  let hash =
    Result.get_ok @@ Multibase.Base32.encode digest |> String.lowercase_ascii
  in
  let did = "did:plc:" ^ String.sub hash 0 24 in
  did

let create_did_credentials (pds_rotation_key : Kleidos.key)
    (signing_did_key : string) ?(rotation_did_keys : string list option) handle
    : credentials =
  let recovery_privkey, (module Rec_curve) = pds_rotation_key in
  let recovery_did_key =
    Rec_curve.pubkey_to_did_key
      (Rec_curve.derive_pubkey ~privkey:recovery_privkey)
  in
  let rotation_keys =
    recovery_did_key :: Option.value rotation_did_keys ~default:[]
  in
  { rotation_keys
  ; verification_methods= [("atproto", signing_did_key)]
  ; also_known_as= ["at://" ^ handle]
  ; services=
      [ ( "atproto_pds"
        , {type'= "AtprotoPersonalDataServer"; endpoint= Env.host_endpoint} ) ]
  }

let get_recommended_credentials ~signing_key ~handle ?(extra_rotation_keys = [])
    () =
  let did_key =
    signing_key |> Kleidos.parse_multikey_str |> Kleidos.derive_pubkey
    |> Kleidos.pubkey_to_did_key
  in
  create_did_credentials Env.rotation_key did_key handle
    ~rotation_did_keys:extra_rotation_keys

let create_did (pds_rotation_key : Kleidos.key) (signing_did_key : string)
    ?(rotation_did_keys : string list option) handle : string * signed_operation
    =
  let {rotation_keys; verification_methods; also_known_as; services} :
      credentials =
    create_did_credentials pds_rotation_key signing_did_key ?rotation_did_keys
      handle
  in
  let operation : unsigned_operation =
    Operation
      { type'= "plc_operation"
      ; rotation_keys
      ; verification_methods
      ; also_known_as
      ; services
      ; prev= None }
  in
  let signed = sign_operation pds_rotation_key operation in
  let did = did_of_operation signed in
  (did, signed)

let submit_genesis ?endpoint (pds_rotation_key : Kleidos.key)
    (signing_did_key : string) ?(rotation_did_keys : string list option) handle
    : (string, string) Lwt_result.t =
  let did, signed =
    create_did pds_rotation_key signing_did_key ?rotation_did_keys handle
  in
  match%lwt submit_operation ?endpoint did signed with
  | Ok _ ->
      Lwt.return_ok did
  | Error (status, error) ->
      Lwt.return_error
      @@ Format.sprintf "error %d while submitting operation %s\n\n%s" status
           (Yojson.Safe.to_string (signed_operation_to_yojson signed))
           error

let get_audit_log ?endpoint did : (audit_log, string) Lwt_result.t =
  let uri =
    Uri.of_string
    @@ Format.sprintf "%s/%s/log/audit"
         (Option.value endpoint ~default:"https://plc.directory")
         did
  in
  let headers = Http.Header.init_with "Accept" "application/json" in
  let%lwt res, body = Util.Http.get ~headers uri in
  match res.status with
  | `OK ->
      let%lwt body = Body.to_string body in
      Lwt.return @@ audit_log_of_yojson @@ Yojson.Safe.from_string body
  | s ->
      let%lwt body_str = Body.to_string body in
      Lwt.return_error
      @@ Format.sprintf "error %d while fetching audit log; %s"
           (Http.Status.to_int s) body_str
