open Cohttp
open Cohttp_lwt
open Cohttp_lwt_unix
open Util.Did_doc_types

let default_endpoint = "https://plc.directory"

type t = {did: string; rotation_key: Kleidos.key; endpoint: string}

type service = {type': string [@key "type"]; endpoint: string}
[@@deriving yojson {strict= false}]

type unsigned_operation =
  | Operation of
      { type': string [@key "type"]
      ; rotation_keys: string list [@key "rotationKeys"]
      ; verification_methods: (string * string) list
            [@key "verificationMethods"]
      ; also_known_as: string list [@key "alsoKnownAs"]
      ; services: (string * service) list
      ; prev: string option }
  | Tombstone of {type': string [@key "type"]; prev: string}
[@@deriving yojson {strict= false}]

let unsigned_operation_to_yojson = function
  | Operation
      {type'; rotation_keys; verification_methods; also_known_as; services; prev}
    ->
      `Assoc
        [ ("type", `String type')
        ; ("rotationKeys", `List (List.map (fun k -> `String k) rotation_keys))
        ; ( "verificationMethods"
          , `Assoc
              (List.map
                 (fun ((k, v) : string * string) -> (k, `String v))
                 verification_methods ) )
        ; ("alsoKnownAs", `List (List.map (fun k -> `String k) also_known_as))
        ; ( "services"
          , `Assoc
              (List.map
                 (fun ((k, v) : string * service) ->
                   ( k
                   , `Assoc
                       [ ("type", `String v.type')
                       ; ("endpoint", `String v.endpoint) ] ) )
                 services ) )
        ; ("prev", match prev with Some p -> `String p | None -> `Null) ]
  | Tombstone {type'; prev} ->
      `Assoc [("type", `String type'); ("prev", `String prev)]

let unsigned_operation_of_yojson (json : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  let type' = json |> member "type" |> to_string in
  let rotation_keys =
    json |> member "rotationKeys" |> to_list |> List.map to_string
  in
  let verification_methods =
    json
    |> member "verificationMethods"
    |> to_assoc
    |> List.map (fun (key, value) -> (key, to_string value))
  in
  let also_known_as =
    json |> member "alsoKnownAs" |> to_list |> List.map to_string
  in
  let services =
    json |> member "services" |> to_assoc
    |> List.map (fun (k, v) -> (k, Result.get_ok @@ service_of_yojson v))
  in
  let prev = json |> member "prev" |> to_string_option in
  match type' with
  | "plc_operation" ->
      Operation
        { type'
        ; rotation_keys
        ; verification_methods
        ; also_known_as
        ; services
        ; prev }
  | "plc_tombstone" ->
      Tombstone {type'; prev= Option.get prev}
  | _ ->
      raise (Invalid_argument "Invalid operation type")

type signed_operation =
  | Operation of
      { type': string [@key "type"]
      ; rotation_keys: string list [@key "rotationKeys"]
      ; verification_methods: (string * string) list
            [@key "verificationMethods"]
      ; also_known_as: string list [@key "alsoKnownAs"]
      ; services: (string * service) list
      ; prev: string option
      ; signature: string [@key "sig"] }
  | Tombstone of
      {type': string [@key "type"]; prev: string; signature: string [@key "sig"]}
[@@deriving yojson {strict= false}]

let signed_operation_to_yojson = function
  | Operation
      { type'
      ; rotation_keys
      ; verification_methods
      ; also_known_as
      ; services
      ; prev
      ; signature } -> (
    match
      unsigned_operation_to_yojson
        (Operation
           { type'
           ; rotation_keys
           ; verification_methods
           ; also_known_as
           ; services
           ; prev } )
    with
    | `Assoc fields ->
        `Assoc (fields @ [("sig", `String signature)])
    | _ ->
        failwith "unexpected json structure" )
  | Tombstone {type'; prev; signature} -> (
    match unsigned_operation_to_yojson (Tombstone {type'; prev}) with
    | `Assoc fields ->
        `Assoc (fields @ [("sig", `String signature)])
    | _ ->
        failwith "unexpected json structure" )

let signed_operation_of_yojson (json : Yojson.Safe.t) =
  let open Yojson.Safe.Util in
  let type' = json |> member "type" |> to_string in
  match type' with
  | "plc_operation" ->
      let rotation_keys =
        json |> member "rotationKeys" |> to_list |> List.map to_string
      in
      let verification_methods =
        json
        |> member "verificationMethods"
        |> to_assoc
        |> List.map (fun (k, v) -> (k, to_string v))
      in
      let also_known_as =
        json |> member "alsoKnownAs" |> to_list |> List.map to_string
      in
      let services =
        json |> member "services" |> to_assoc
        |> List.map (fun (k, v) -> (k, Result.get_ok @@ service_of_yojson v))
      in
      let prev = json |> member "prev" |> to_string_option in
      let signature = json |> member "sig" |> to_string in
      Ok
        (Operation
           { type'
           ; rotation_keys
           ; verification_methods
           ; also_known_as
           ; services
           ; prev
           ; signature } )
  | "plc_tombstone" ->
      let prev = json |> member "prev" |> to_string in
      let signature = json |> member "sig" |> to_string in
      Ok (Tombstone {type'; prev; signature})
  | t ->
      Error ("unexpected operation type " ^ t)

type audit_log_operation =
  { signature: string [@key "sig"]
  ; prev: string option
  ; type': string [@key "type"]
  ; services: (string * service) list
        [@to_yojson
          fun l -> `Assoc (List.map (fun (k, v) -> (k, service_to_yojson v)) l)]
        [@of_yojson
          function
          | `Assoc fields ->
              Ok
                (List.filter_map
                   (fun (k, v) ->
                     match service_of_yojson v with
                     | Ok service ->
                         Some (k, service)
                     | _ ->
                         None )
                   fields )
          | _ ->
              Error "Expected object for services"]
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
      Lwt.return_ok ()
  | _ ->
      let%lwt body_str = Body.to_string body in
      Lwt.return_error (Http.Status.to_int res.status, body_str)

let did_of_operation operation : string =
  let cbor = signed_operation_to_yojson operation |> Dag_cbor.encode_yojson in
  let digest = Digestif.SHA256.(cbor |> digest_bytes |> to_raw_string) in
  let hash =
    Result.get_ok @@ Multibase.Base32.encode digest |> String.lowercase_ascii
  in
  let did = "did:plc:" ^ String.sub hash 0 24 in
  did

let create_did (pds_rotation_key : Kleidos.key) (signing_did_key : string)
    ?(rotation_did_keys : string list option) handle : string * signed_operation
    =
  let recovery_privkey, (module Rec_curve) = pds_rotation_key in
  let recovery_did_key =
    Rec_curve.pubkey_to_did_key
      (Rec_curve.derive_pubkey ~privkey:recovery_privkey)
  in
  let rotation_keys =
    recovery_did_key :: Option.value rotation_did_keys ~default:[]
  in
  let operation : unsigned_operation =
    Operation
      { type'= "plc_operation"
      ; rotation_keys
      ; verification_methods= [("atproto", signing_did_key)]
      ; also_known_as= ["at://" ^ handle]
      ; services=
          [ ( "atproto_pds"
            , { type'= "AtprotoPersonalDataServer"
              ; endpoint= "https://" ^ Env.hostname } ) ]
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
  let%lwt res, body = Client.get ~headers uri in
  match res.status with
  | `OK ->
      let%lwt body = Body.to_string body in
      Lwt.return @@ audit_log_of_yojson @@ Yojson.Safe.from_string body
  | s ->
      let%lwt body_str = Body.to_string body in
      Lwt.return_error
      @@ Format.sprintf "error %d while fetching audit log; %s"
           (Http.Status.to_int s) body_str
