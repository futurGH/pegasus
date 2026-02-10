module Constants = Constants
module Syntax = Syntax
module Rapper = Rapper_
module Types = Types
module Sqlite = Sqlite_
module Time = Time
module Http = Http_
module Html = Html

(* returns all blob refs in a record *)
let rec find_blob_refs (record : Mist.Lex.repo_record) : Mist.Blob_ref.t list =
  let rec aux acc entries =
    List.fold_left
      (fun acc value ->
        match value with
        | `BlobRef blob ->
            blob :: acc
        | `LexMap map ->
            find_blob_refs map @ acc
        | `LexArray arr ->
            aux acc (Array.to_list arr) @ acc
        | _ ->
            acc )
      acc entries
  in
  aux [] (Mist.Lex.String_map.bindings record |> List.map snd)
  |> List.sort_uniq (fun (r1 : Mist.Blob_ref.t) r2 -> Cid.compare r1.ref r2.ref)

let mkfile_p path ~perm =
  Core_unix.mkdir_p (Filename.dirname path) ~perm:0o755 ;
  Core_unix.openfile ~mode:[O_CREAT; O_WRONLY] ~perm path |> Core_unix.close

let sig_matches_some_did_key ~did_keys ~signature ~msg =
  List.find_opt
    (fun key ->
      let raw, (module Curve) =
        Kleidos.parse_multikey_str (String.sub key 8 (String.length key - 8))
      in
      let valid =
        Curve.verify ~pubkey:(Curve.normalize_pubkey_to_raw raw) ~signature ~msg
      in
      valid )
    did_keys
  <> None

let request_ip req =
  Dream.header req "X-Forwarded-For"
  |> Option.value ~default:(Dream.client req)
  |> String.split_on_char ',' |> List.hd |> String.split_on_char ':' |> List.hd
  |> String.trim

let str_contains ~affix str =
  let re = Str.regexp_string affix in
  try
    ignore (Str.search_forward re str 0) ;
    true
  with Not_found -> false

let make_code () =
  let () = Mirage_crypto_rng_unix.use_default () in
  let token =
    Multibase.Base32.encode_string ~pad:false
    @@ Mirage_crypto_rng_unix.getrandom 8
  in
  String.sub token 0 5 ^ "-" ^ String.sub token 5 5

let send_email_or_log ~(recipients : Letters.recipient list) ~subject
    ~(body : Letters.body) =
  let log_email () =
    match body with
    | Plain text | Html text | Mixed (text, _, _) ->
        let to_addr =
          List.find_map
            (fun (r : Letters.recipient) ->
              match r with To addr -> Some addr | _ -> None )
            recipients
          |> Option.get
        in
        Log.info (fun log -> log "email to %s: %s" to_addr text)
  in
  match (Env.smtp_config, Env.smtp_sender) with
  | Some config, Some sender -> (
    match Letters.create_email ~from:sender ~recipients ~subject ~body () with
    | Error e ->
        failwith (Printf.sprintf "failed to construct email: %s" e)
    | Ok message -> (
      try%lwt Letters.send ~config ~sender ~recipients ~message
      with e ->
        Log.log_exn e ;
        Lwt.return (log_email ()) ) )
  | _ ->
      Lwt.return (log_email ())

let s3_error_to_string : Aws_s3_lwt.S3.error -> string = function
  | Redirect endpoint ->
      "redirect to " ^ endpoint.host
  | Throttled ->
      "throttled"
  | Unknown (code, msg) ->
      Printf.sprintf "unknown error %d: %s" code msg
  | Failed exn ->
      Printf.sprintf "failed: %s" (Printexc.to_string exn)
  | Forbidden ->
      "forbidden"
  | Not_found ->
      "not found"
