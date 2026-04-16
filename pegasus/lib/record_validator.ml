module Types = Hermes_cli.Lexicon_types

exception Validation_error of string

let fail fmt = Printf.ksprintf (fun msg -> raise (Validation_error msg)) fmt

type ctx = {current_doc: Types.lexicon_doc; visited: (string * string) list}

let count_graphemes s =
  Uuseg_string.fold_utf_8 `Grapheme_cluster (fun acc _ -> acc + 1) 0 s

let compile_re s = Re.Pcre.re s |> Re.compile

let did_re = compile_re {|^did:[a-z]+:[a-zA-Z0-9._:%-]+$|}

let is_did s = String.length s <= 2048 && Re.execp did_re s

let handle_label_re =
  compile_re {|^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$|}

let is_handle s =
  let len = String.length s in
  if len < 1 || len > 253 then false
  else
    let labels = String.split_on_char '.' s in
    match labels with
    | [] | [_] ->
        false
    | _ ->
        let rec walk = function
          | [] ->
              false (* unreachable *)
          | [last] ->
              (* final label (TLD) must start with a letter *)
              let c = if last = "" then ' ' else last.[0] in
              ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))
              && Re.execp handle_label_re last
          | l :: rest ->
              Re.execp handle_label_re l && walk rest
        in
        walk labels

let is_at_identifier s = is_did s || is_handle s

let nsid_label_re = compile_re {|^[a-zA-Z](?:[a-zA-Z0-9-]{0,62})$|}

let nsid_name_re = compile_re {|^[a-zA-Z](?:[a-zA-Z0-9]{0,62})$|}

let is_nsid s =
  let len = String.length s in
  if len < 3 || len > 317 then false
  else
    let labels = String.split_on_char '.' s in
    match List.rev labels with
    | [] | [_] | [_; _] ->
        false
    | name :: authority_rev ->
        Re.execp nsid_name_re name
        && List.for_all (fun l -> Re.execp nsid_label_re l) authority_rev

let tid_re =
  compile_re {|^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$|}

let is_tid s = Re.execp tid_re s

let record_key_re = compile_re {|^[a-zA-Z0-9_~.:-]{1,512}$|}

let is_record_key s =
  s <> "." && s <> ".." && String.length s <= 512 && Re.execp record_key_re s

let datetime_re =
  compile_re
    {|^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$|}

let is_datetime s = Re.execp datetime_re s

let is_at_uri s = Option.is_some (Util.Syntax.parse_at_uri s)

let is_cid s = Result.is_ok (Cid.of_string s)

let validate_string_format fmt s =
  let fmt_err () = fail "value %S is not a valid %s" s fmt in
  match fmt with
  | "at-identifier" ->
      if not (is_at_identifier s) then fmt_err ()
  | "did" ->
      if not (is_did s) then fmt_err ()
  | "handle" ->
      if not (is_handle s) then fmt_err ()
  | "nsid" ->
      if not (is_nsid s) then fmt_err ()
  | "tid" ->
      if not (is_tid s) then fmt_err ()
  | "record-key" ->
      if not (is_record_key s) then fmt_err ()
  | "datetime" ->
      if not (is_datetime s) then fmt_err ()
  | "at-uri" ->
      if not (is_at_uri s) then fmt_err ()
  | "cid" ->
      if not (is_cid s) then fmt_err ()
  | "uri" | "language" ->
      (* validation is complex, will implement later maybe *)
      ()
  | _ ->
      (* unknown format; accept *)
      ()

type parsed_ref = {nsid: string option; fragment: string}

let parse_ref r =
  match String.index_opt r '#' with
  | None ->
      {nsid= Some r; fragment= "main"}
  | Some 0 ->
      {nsid= None; fragment= String.sub r 1 (String.length r - 1)}
  | Some i ->
      { nsid= Some (String.sub r 0 i)
      ; fragment= String.sub r (i + 1) (String.length r - i - 1) }

let lookup_def (doc : Types.lexicon_doc) name =
  List.find_opt (fun d -> d.Types.name = name) doc.defs
  |> Option.map (fun d -> d.Types.type_def)

(* returns (doc, type_def) that the ref points at, plus the effective nsid
   so we can register it in the visited set *)
let resolve_ref ctx ref_str =
  let pr = parse_ref ref_str in
  let target_nsid = Option.value pr.nsid ~default:ctx.current_doc.id in
  if target_nsid = ctx.current_doc.id then
    match lookup_def ctx.current_doc pr.fragment with
    | Some td ->
        Lwt.return (target_nsid, ctx.current_doc, td)
    | None ->
        fail "ref %s: no def %s in lexicon" ref_str pr.fragment
  else
    match%lwt Lexicon_resolver.resolve_schema target_nsid with
    | Error e ->
        fail "ref %s: could not resolve lexicon: %s" ref_str e
    | Ok other_doc -> (
      match lookup_def other_doc pr.fragment with
      | Some td ->
          Lwt.return (target_nsid, other_doc, td)
      | None ->
          fail "ref %s: no def %s in lexicon %s" ref_str pr.fragment target_nsid
      )

let validate_string (spec : Types.string_spec) json =
  let s = match json with `String s -> s | _ -> fail "expected string" in
  ( match spec.const with
  | Some c when s <> c ->
      fail "expected constant %S, got %S" c s
  | _ ->
      () ) ;
  ( match spec.enum with
  | Some vs when not (List.mem s vs) ->
      fail "value %S not in enum" s
  | _ ->
      () ) ;
  ( match spec.min_length with
  | Some n when String.length s < n ->
      fail "string shorter than minLength %d" n
  | _ ->
      () ) ;
  ( match spec.max_length with
  | Some n when String.length s > n ->
      fail "string longer than maxLength %d" n
  | _ ->
      () ) ;
  ( match (spec.min_graphemes, spec.max_graphemes) with
  | None, None ->
      ()
  | mn, mx -> (
      let g = count_graphemes s in
      ( match mn with
      | Some n when g < n ->
          fail "string shorter than minGraphemes %d" n
      | _ ->
          () ) ;
      match mx with
      | Some n when g > n ->
          fail "string longer than maxGraphemes %d" n
      | _ ->
          () ) ) ;
  match spec.format with Some f -> validate_string_format f s | None -> ()

let validate_integer (spec : Types.integer_spec) json =
  let i =
    match json with
    | `Int i ->
        i
    | `Intlit s -> (
      try int_of_string s with _ -> fail "invalid integer literal %s" s )
    | _ ->
        fail "expected integer"
  in
  ( match spec.const with
  | Some c when i <> c ->
      fail "expected constant %d, got %d" c i
  | _ ->
      () ) ;
  ( match spec.enum with
  | Some vs when not (List.mem i vs) ->
      fail "value %d not in enum" i
  | _ ->
      () ) ;
  ( match spec.minimum with
  | Some n when i < n ->
      fail "integer below minimum %d" n
  | _ ->
      () ) ;
  match spec.maximum with
  | Some n when i > n ->
      fail "integer above maximum %d" n
  | _ ->
      ()

let validate_boolean (spec : Types.boolean_spec) json =
  let b = match json with `Bool b -> b | _ -> fail "expected boolean" in
  match spec.const with
  | Some c when b <> c ->
      fail "expected constant %b, got %b" c b
  | _ ->
      ()

let validate_bytes (spec : Types.bytes_spec) json =
  let b64 =
    match json with
    | `Assoc [("$bytes", `String s)] ->
        s
    | _ ->
        fail "expected bytes object with $bytes field"
  in
  let byte_len =
    (* 3 bytes per 4 b64 chars minus padding *)
    let n = String.length b64 in
    let pad =
      if n >= 2 && String.sub b64 (n - 2) 2 = "==" then 2
      else if n >= 1 && String.sub b64 (n - 1) 1 = "=" then 1
      else 0
    in
    (n / 4 * 3) - pad
  in
  ( match spec.min_length with
  | Some n when byte_len < n ->
      fail "bytes shorter than minLength %d" n
  | _ ->
      () ) ;
  match spec.max_length with
  | Some n when byte_len > n ->
      fail "bytes longer than maxLength %d" n
  | _ ->
      ()

let validate_cid_link json =
  match json with
  | `Assoc [("$link", `String s)] ->
      if not (is_cid s) then fail "invalid CID in $link"
  | _ ->
      fail "expected cid-link object {\"$link\": ...}"

let mime_matches pattern mime =
  if pattern = "*/*" then true
  else
    match String.index_opt pattern '/' with
    | None ->
        pattern = mime
    | Some i -> (
        let p_type = String.sub pattern 0 i in
        let p_sub =
          String.sub pattern (i + 1) (String.length pattern - i - 1)
        in
        match String.index_opt mime '/' with
        | None ->
            false
        | Some j ->
            let m_type = String.sub mime 0 j in
            let m_sub = String.sub mime (j + 1) (String.length mime - j - 1) in
            p_type = m_type && (p_sub = "*" || p_sub = m_sub) )

let validate_blob (spec : Types.blob_spec) json =
  let fields =
    match json with `Assoc pairs -> pairs | _ -> fail "expected blob object"
  in
  let get k = List.assoc_opt k fields in
  let mime_type =
    match get "mimeType" with
    | Some (`String s) ->
        s
    | _ ->
        fail "blob missing mimeType"
  in
  let size =
    match get "$type" with
    | Some (`String "blob") -> (
        ( match get "ref" with
        | Some (`Assoc [("$link", `String c)]) ->
            if not (is_cid c) then fail "blob ref has invalid CID"
        | _ ->
            fail "blob missing ref" ) ;
        match get "size" with
        | Some (`Int n) ->
            Some n
        | Some (`Intlit s) -> (
          try Some (int_of_string s) with _ -> fail "invalid blob size" )
        | _ ->
            fail "blob missing size" )
    | _ -> (
      (* legacy shape *)
      match get "cid" with
      | Some (`String c) ->
          if not (is_cid c) then fail "blob cid is invalid" ;
          None
      | _ ->
          fail "blob has neither $type=blob nor legacy cid field" )
  in
  ( match spec.accept with
  | Some [] | None ->
      ()
  | Some patterns ->
      if not (List.exists (fun p -> mime_matches p mime_type) patterns) then
        fail "blob mimeType %S not in accept list" mime_type ) ;
  match (spec.max_size, size) with
  | Some max_s, Some s when s > max_s ->
      fail "blob size %d exceeds maxSize %d" s max_s
  | _ ->
      ()

let rec validate_value ctx (td : Types.type_def) (json : Yojson.Safe.t) :
    unit Lwt.t =
  match td with
  | String spec ->
      validate_string spec json ; Lwt.return_unit
  | Integer spec ->
      validate_integer spec json ; Lwt.return_unit
  | Boolean spec ->
      validate_boolean spec json ; Lwt.return_unit
  | Bytes spec ->
      validate_bytes spec json ; Lwt.return_unit
  | Blob spec ->
      validate_blob spec json ; Lwt.return_unit
  | CidLink _ ->
      validate_cid_link json ; Lwt.return_unit
  | Array spec ->
      validate_array ctx spec json
  | Object spec ->
      validate_object ctx spec json
  | Ref spec ->
      validate_ref ctx spec json
  | Union spec ->
      validate_union ctx spec json
  | Token _ ->
      (match json with `String _ -> () | _ -> fail "expected token (string)") ;
      Lwt.return_unit
  | Unknown _ ->
      Lwt.return_unit
  | Record spec ->
      validate_object ctx spec.record json
  | Query _ | Procedure _ | Subscription _ | PermissionSet _ ->
      fail "lexicon type %s is not a valid record payload"
        ( match td with
        | Query _ ->
            "query"
        | Procedure _ ->
            "procedure"
        | Subscription _ ->
            "subscription"
        | PermissionSet _ ->
            "permission-set"
        | _ ->
            "other" )

and validate_array ctx (spec : Types.array_spec) json =
  let items = match json with `List xs -> xs | _ -> fail "expected array" in
  let len = List.length items in
  ( match spec.min_length with
  | Some n when len < n ->
      fail "array shorter than minLength %d" n
  | _ ->
      () ) ;
  ( match spec.max_length with
  | Some n when len > n ->
      fail "array longer than maxLength %d" n
  | _ ->
      () ) ;
  Lwt_list.iter_s (validate_value ctx spec.items) items

and validate_object ctx (spec : Types.object_spec) json =
  let fields =
    match json with `Assoc kvs -> kvs | _ -> fail "expected object"
  in
  let required = Option.value spec.required ~default:[] in
  let nullable = Option.value spec.nullable ~default:[] in
  List.iter
    (fun k ->
      if not (List.mem_assoc k fields) then fail "missing required field %S" k )
    required ;
  Lwt_list.iter_s
    (fun (name, (prop : Types.property)) ->
      match List.assoc_opt name fields with
      | None ->
          Lwt.return_unit
      | Some `Null when List.mem name nullable ->
          Lwt.return_unit
      | Some `Null ->
          fail "field %S is not nullable" name
      | Some v ->
          Lwt.catch
            (fun () -> validate_value ctx prop.type_def v)
            (function
              | Validation_error m -> fail "at %S: %s" name m | e -> Lwt.fail e ) )
    spec.properties

and validate_ref ctx (spec : Types.ref_spec) json =
  let pr = parse_ref spec.ref_ in
  let target_nsid = Option.value pr.nsid ~default:ctx.current_doc.id in
  let key = (target_nsid, pr.fragment) in
  if List.mem key ctx.visited then Lwt.return_unit
  else
    let%lwt _nsid, doc, td = resolve_ref ctx spec.ref_ in
    let ctx' = {current_doc= doc; visited= key :: ctx.visited} in
    (* A ref may resolve to a Record def; record's content is the inner object. *)
    let td =
      match td with Types.Record {record; _} -> Types.Object record | t -> t
    in
    validate_value ctx' td json

and validate_union ctx (spec : Types.union_spec) json =
  let closed = Option.value spec.closed ~default:false in
  let fields =
    match json with `Assoc kvs -> kvs | _ -> fail "expected object in union"
  in
  let type_tag =
    match List.assoc_opt "$type" fields with
    | Some (`String s) ->
        Some s
    | _ ->
        None
  in
  let ref_matches_tag tag ref_str =
    let pr = parse_ref ref_str in
    let ref_nsid = Option.value pr.nsid ~default:ctx.current_doc.id in
    let canonical =
      if pr.fragment = "main" then ref_nsid else ref_nsid ^ "#" ^ pr.fragment
    in
    canonical = tag
  in
  match type_tag with
  | None ->
      if closed then fail "union value missing $type (closed union)"
      else Lwt.return_unit
  | Some tag -> (
    match List.find_opt (ref_matches_tag tag) spec.refs with
    | Some matched ->
        let fake_ref : Types.ref_spec = {ref_= matched; description= None} in
        validate_ref ctx fake_ref json
    | None ->
        if closed then fail "$type %S not in closed union" tag
        else Lwt.return_unit )

let validate_record ~nsid ~(record : Yojson.Safe.t) =
  match%lwt Lexicon_resolver.resolve_schema nsid with
  | Error e ->
      Lwt.return_error ("could not resolve lexicon: " ^ e)
  | Ok doc -> (
    match lookup_def doc "main" with
    | None ->
        Lwt.return_error "lexicon has no defs.main"
    | Some (Record rec_spec) ->
        let ctx = {current_doc= doc; visited= [(nsid, "main")]} in
        Lwt.catch
          (fun () ->
            let%lwt () = validate_object ctx rec_spec.record record in
            Lwt.return_ok () )
          (function
            | Validation_error msg ->
                Lwt.return_error msg
            | e ->
                Lwt.return_error (Printexc.to_string e) )
    | Some _ ->
        Lwt.return_error "defs.main is not a record type" )
