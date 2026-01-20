type permission =
  { resource: string
  ; lxm: string list option [@default None]
  ; aud: string option [@default None]
  ; inherit_aud: bool option [@key "inheritAud"] [@default None]
  ; collection: string list option [@default None]
  ; action: string list option [@default None]
  ; accept: string list option [@default None] }
[@@deriving yojson {strict= false}]

type permission_set =
  { title: string option [@default None]
  ; title_lang: (string * string) list option [@default None]
  ; detail: string option [@default None]
  ; detail_lang: (string * string) list option [@default None]
  ; permissions: permission list }
[@@deriving yojson {strict= false}]

type lexicon_value =
  { type_: string [@key "$type"]
  ; title: string option [@default None]
  ; detail: string option [@default None]
  ; permissions: permission list option [@default None] }
[@@deriving yojson {strict= false}]

let cache : permission_set Ttl_cache.String_cache.t =
  Ttl_cache.String_cache.create (3 * Util.Time.hour) ()

(* reuse dns client from id_resolver *)
let dns_client = Id_resolver.Handle.dns_client

(* resolve did authority for nsid *)
let resolve_did_authority nsid =
  let authority = Util.Syntax.nsid_authority nsid in
  try%lwt
    let%lwt result =
      Dns_client_lwt.getaddrinfo dns_client Dns.Rr_map.Txt
        (Domain_name.of_string_exn ("_lexicon." ^ authority))
    in
    match result with
    | Ok (_, t) -> (
        let txt = Dns.Rr_map.Txt_set.choose t in
        match String.split_on_char '=' txt with
        | ["did"; did]
          when String.starts_with ~prefix:"did:plc:" did
               || String.starts_with ~prefix:"did:web:" did ->
            Lwt.return_ok (String.trim did)
        | _ ->
            Lwt.return_error "invalid did in dns record" )
    | Error (`Msg e) ->
        Lwt.return_error e
  with exn -> Lwt.return_error (Printexc.to_string exn)

(* fetch lexicon document from authority's repo *)
let fetch_lexicon ~did ~nsid =
  try%lwt
    match%lwt Id_resolver.Did.resolve did with
    | Error e ->
        Lwt.return_error ("failed to resolve DID: " ^ e)
    | Ok doc -> (
      match Id_resolver.Did.Document.get_service doc "#atproto_pds" with
      | None ->
          Lwt.return_error "no PDS service in DID document"
      | Some pds -> (
          let client = Hermes.make_client ~service:pds () in
          try%lwt
            let%lwt record =
              Lexicons.([%xrpc get "com.atproto.repo.getRecord"])
                ~repo:did ~collection:"com.atproto.lexicon.schema" ~rkey:nsid
                client
            in
            Lwt.return_ok record.value
          with _ -> Lwt.return_error ("failed to fetch lexicon record " ^ nsid)
          ) )
  with exn -> Lwt.return_error (Printexc.to_string exn)

(* parse lexicon record into permission_set *)
let parse_permission_set record =
  match lexicon_value_of_yojson record with
  | Error e ->
      Error ("failed to parse lexicon record: " ^ e)
  | Ok record -> (
      if record.type_ <> "permission-set" then
        Error ("not a permission-set lexicon: " ^ record.type_)
      else
        match record.permissions with
        | None ->
            Error "permission-set has no permissions"
        | Some permissions ->
            Ok
              { title= record.title
              ; title_lang= None (* skip localized titles for now *)
              ; detail= record.detail
              ; detail_lang= None (* skip localized details for now *)
              ; permissions } )

(* resolve and parse permission set from nsid *)
let resolve nsid =
  match Ttl_cache.String_cache.get cache nsid with
  | Some cached ->
      Lwt.return_ok cached
  | None -> (
    match%lwt resolve_did_authority nsid with
    | Error e ->
        Lwt.return_error ("DNS resolution failed: " ^ e)
    | Ok did -> (
      match%lwt fetch_lexicon ~did ~nsid with
      | Error e ->
          Lwt.return_error ("lexicon fetch failed: " ^ e)
      | Ok json -> (
        match parse_permission_set json with
        | Error e ->
            Lwt.return_error e
        | Ok ps ->
            Ttl_cache.String_cache.set cache nsid ps ;
            Lwt.return_ok ps ) ) )

let clear_cache nsid = Ttl_cache.String_cache.remove cache nsid
