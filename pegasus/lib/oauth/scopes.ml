type account_attr = Email | Repo

type account_action = Read | Manage

type account_permission = {attr: account_attr; actions: account_action list}

type identity_attr = Handle | Any

type identity_permission = {attr: identity_attr}

type repo_action = Create | Update | Delete

let show_repo_action = function
  | Create ->
      "create"
  | Update ->
      "update"
  | Delete ->
      "delete"

type repo_collection = All | Collection of string

type repo_permission =
  {collections: repo_collection list; actions: repo_action list}

type rpc_lxm = AnyLxm | Lxm of string

type rpc_aud = AnyAud | Aud of string

type rpc_permission = {lxm: rpc_lxm list; aud: rpc_aud}

type accept_pattern =
  | AnyMime  (** */* *)
  | TypeWildcard of string  (** e.g. image/* *)
  | ExactMime of string * string  (** e.g. image/png *)

type blob_permission = {accept: accept_pattern list}

type include_scope = {nsid: string; aud: string option}

type static_scope =
  | Atproto
  | TransitionEmail
  | TransitionGeneric
  | TransitionChatBsky

type scope =
  | Static of static_scope
  | Account of account_permission
  | Identity of identity_permission
  | Repo of repo_permission
  | Rpc of rpc_permission
  | Blob of blob_permission
  | Include of include_scope

let is_valid_nsid s =
  let segments = String.split_on_char '.' s in
  let valid_segment seg =
    String.length seg > 0
    && String.for_all
         (fun c ->
           (c >= 'a' && c <= 'z')
           || (c >= 'A' && c <= 'Z')
           || (c >= '0' && c <= '9')
           || c = '-' )
         seg
  in
  List.length segments >= 3 && List.for_all valid_segment segments

(* check if permission_nsid is under include_nsid's authority *)
let is_parent_authority_of ~include_nsid ~permission_nsid =
  let include_authority = Util.Syntax.nsid_authority include_nsid in
  let permission_authority = Util.Syntax.nsid_authority permission_nsid in
  String.equal include_authority permission_authority
  || String.starts_with ~prefix:(include_authority ^ ".") permission_authority

let parse_params s =
  if s = "" then []
  else
    String.split_on_char '&' s
    |> List.filter_map (fun pair ->
        match String.split_on_char '=' pair with
        | [k; v] ->
            Some (Uri.pct_decode k, Uri.pct_decode v)
        | [k] ->
            Some (Uri.pct_decode k, "")
        | _ ->
            None )

let get_all_params key params =
  List.filter_map (fun (k, v) -> if k = key then Some v else None) params

let get_single_param key params =
  match get_all_params key params with [v] -> Some v | _ -> None

let parse_scope_syntax s =
  let qmark_idx = String.index_opt s '?' in
  let colon_idx = String.index_opt s ':' in
  let prefix_end =
    match (qmark_idx, colon_idx) with
    | None, None ->
        String.length s
    | Some q, None ->
        q
    | None, Some c ->
        c
    | Some q, Some c ->
        min q c
  in
  let prefix = String.sub s 0 prefix_end in
  let positional =
    match colon_idx with
    | Some c when c = prefix_end ->
        let end_pos =
          match qmark_idx with Some q -> q | None -> String.length s
        in
        if end_pos > c + 1 then
          Some (Uri.pct_decode (String.sub s (c + 1) (end_pos - c - 1)))
        else None
    | _ ->
        None
  in
  let params =
    match qmark_idx with
    | Some q when q < String.length s - 1 ->
        parse_params (String.sub s (q + 1) (String.length s - q - 1))
    | _ ->
        []
  in
  (prefix, positional, params)

let parse_account_attr = function
  | "email" ->
      Some Email
  | "repo" ->
      Some Repo
  | _ ->
      None

let parse_account_action = function
  | "read" ->
      Some Read
  | "manage" ->
      Some Manage
  | _ ->
      None

let parse_account_permission positional params =
  let attr = positional |> Option.map parse_account_attr |> Option.join in
  let action_strs = get_all_params "action" params in
  let actions =
    if action_strs = [] then [Read]
    else List.filter_map parse_account_action action_strs
  in
  match (actions, attr) with
  | _action :: _, Some attr ->
      Some {attr; actions}
  | _ ->
      None

let parse_identity_attr = function
  | "handle" ->
      Some Handle
  | "*" ->
      Some Any
  | _ ->
      None

let parse_identity_permission positional _params =
  positional
  |> Option.map parse_identity_attr
  |> Option.join
  |> Option.map (fun attr -> {attr})

let parse_repo_action = function
  | "create" ->
      Some Create
  | "update" ->
      Some Update
  | "delete" ->
      Some Delete
  | _ ->
      None

let all_repo_actions = [Create; Update; Delete]

let parse_repo_collection s =
  if s = "*" then Some All
  else if is_valid_nsid s then Some (Collection s)
  else None

let parse_repo_permission positional params =
  (* duplicate positional and query parameters not allowed *)
  let has_collection_param = get_all_params "collection" params <> [] in
  if positional <> None && has_collection_param then None
  else
    let collection_strs =
      match positional with
      | Some p ->
          [p]
      | None ->
          get_all_params "collection" params
    in
    if collection_strs = [] then None
    else
      let collections = List.filter_map parse_repo_collection collection_strs in
      if collections = [] then None
      else
        let action_strs = get_all_params "action" params in
        let actions =
          if action_strs = [] then all_repo_actions
          else List.filter_map parse_repo_action action_strs
        in
        if actions = [] then None else Some {collections; actions}

let parse_rpc_lxm s =
  if s = "*" then Some AnyLxm
  else if is_valid_nsid s then Some (Lxm s)
  else None

let is_valid_atproto_audience s =
  let parts = String.split_on_char '#' s in
  match parts with
  | [did] | [did; _] -> (
    match String.split_on_char ':' did with
    | "did" :: method_ :: _ when String.length method_ > 0 ->
        true
    | _ ->
        false )
  | _ ->
      false

let parse_rpc_aud s =
  if s = "*" then Some AnyAud
  else if is_valid_atproto_audience s then Some (Aud s)
  else None

let parse_rpc_permission positional params =
  (* duplicate positional and query parameters not allowed *)
  let has_lxm_param = get_all_params "lxm" params <> [] in
  if positional <> None && has_lxm_param then None
  else
    let lxm_strs =
      match positional with
      | Some p ->
          [p]
      | None ->
          get_all_params "lxm" params
    in
    if lxm_strs = [] then None
    else
      let lxms = List.filter_map parse_rpc_lxm lxm_strs in
      if lxms = [] then None
      else
        match get_single_param "aud" params with
        | None ->
            None (* aud is required *)
        | Some aud_str -> (
          match parse_rpc_aud aud_str with
          | None ->
              None
          | Some aud ->
              (* rpc:*?aud=* is forbidden *)
              if aud = AnyAud && List.mem AnyLxm lxms then None
              else Some {lxm= lxms; aud} )

let parse_accept_pattern s =
  if s = "*/*" then Some AnyMime
  else
    match String.split_on_char '/' s with
    | [type_; "*"] when String.length type_ > 0 ->
        Some (TypeWildcard type_)
    | [type_; subtype]
      when String.length type_ > 0
           && String.length subtype > 0
           && (not (String.contains type_ '*'))
           && not (String.contains subtype '*') ->
        Some (ExactMime (type_, subtype))
    | _ ->
        None

let parse_blob_permission positional params =
  (* duplicate positional and query parameters not allowed *)
  let has_accept_param = get_all_params "accept" params <> [] in
  if positional <> None && has_accept_param then None
  else
    let accept_strs =
      match positional with
      | Some p ->
          [p]
      | None ->
          get_all_params "accept" params
    in
    if accept_strs = [] then None
    else
      let accepts = List.filter_map parse_accept_pattern accept_strs in
      if accepts = [] then None else Some {accept= accepts}

let parse_include_scope positional params =
  match positional with
  | None ->
      None
  | Some nsid -> (
      if not (is_valid_nsid nsid) then None
      else
        let aud = get_single_param "aud" params in
        (* validate aud if present *)
        match aud with
        | Some a when not (is_valid_atproto_audience a) ->
            None
        | _ ->
            Some {nsid; aud} )

let parse_static_scope = function
  | "atproto" ->
      Some (Static Atproto)
  | "transition:email" ->
      Some (Static TransitionEmail)
  | "transition:generic" ->
      Some (Static TransitionGeneric)
  | "transition:chat.bsky" ->
      Some (Static TransitionChatBsky)
  | _ ->
      None

let parse_scope s =
  match parse_static_scope s with
  | Some scope ->
      Some scope
  | None -> (
      let prefix, positional, params = parse_scope_syntax s in
      match prefix with
      | "account" ->
          Option.map
            (fun p -> Account p)
            (parse_account_permission positional params)
      | "identity" ->
          Option.map
            (fun p -> Identity p)
            (parse_identity_permission positional params)
      | "repo" ->
          Option.map (fun p -> Repo p) (parse_repo_permission positional params)
      | "rpc" ->
          Option.map (fun p -> Rpc p) (parse_rpc_permission positional params)
      | "blob" ->
          Option.map (fun p -> Blob p) (parse_blob_permission positional params)
      | "include" ->
          Option.map
            (fun p -> Include p)
            (parse_include_scope positional params)
      | _ ->
          None )

let parse_scopes s =
  if s = "" then []
  else
    String.split_on_char ' ' s
    |> List.filter (fun s -> s <> "")
    |> List.filter_map parse_scope

type account_match = {attr: account_attr; action: account_action}

type identity_match = {attr: identity_attr}

type repo_match = {collection: string; action: repo_action}

type rpc_match = {lxm: string; aud: string}

type blob_match = {mime: string}

let account_permission_matches (perm : account_permission) (opts : account_match)
    =
  perm.attr = opts.attr
  && (List.mem Manage perm.actions || List.mem opts.action perm.actions)

let identity_permission_matches (perm : identity_permission)
    (opts : identity_match) =
  perm.attr = Any || perm.attr = opts.attr

let repo_permission_matches perm (opts : repo_match) =
  List.mem opts.action perm.actions
  && ( List.mem All perm.collections
     || List.mem (Collection opts.collection) perm.collections )

let rpc_permission_matches (perm : rpc_permission) (opts : rpc_match) =
  (perm.aud = AnyAud || perm.aud = Aud opts.aud)
  && (List.mem AnyLxm perm.lxm || List.mem (Lxm opts.lxm) perm.lxm)

let accept_matches_mime pattern mime =
  match pattern with
  | AnyMime ->
      true
  | TypeWildcard t -> (
    match String.split_on_char '/' mime with
    | [type_; _subtype]
      when (not (String.contains type_ '*'))
           && not (String.contains _subtype '*') ->
        String.lowercase_ascii type_ = String.lowercase_ascii t
    | _ ->
        false )
  | ExactMime (t, s) -> (
    match String.split_on_char '/' mime with
    | [type_; subtype]
      when (not (String.contains type_ '*'))
           && not (String.contains subtype '*') ->
        String.lowercase_ascii type_ = String.lowercase_ascii t
        && String.lowercase_ascii subtype = String.lowercase_ascii s
    | _ ->
        false )

let blob_permission_matches perm (opts : blob_match) =
  List.exists (fun pat -> accept_matches_mime pat opts.mime) perm.accept

type t = scope list

let of_string s = parse_scopes s

let of_list strs = List.filter_map parse_scope strs

let allows_account scopes opts =
  List.exists
    (fun scope ->
      match scope with
      | Account perm ->
          account_permission_matches perm opts
      | _ ->
          false )
    scopes

let allows_identity scopes opts =
  List.exists
    (fun scope ->
      match scope with
      | Identity perm ->
          identity_permission_matches perm opts
      | _ ->
          false )
    scopes

let allows_repo scopes opts =
  List.exists
    (fun scope ->
      match scope with
      | Repo perm ->
          repo_permission_matches perm opts
      | _ ->
          false )
    scopes

let allows_rpc scopes opts =
  List.exists
    (fun scope ->
      match scope with
      | Rpc perm ->
          rpc_permission_matches perm opts
      | _ ->
          false )
    scopes

let allows_blob scopes opts =
  List.exists
    (fun scope ->
      match scope with
      | Blob perm ->
          blob_permission_matches perm opts
      | _ ->
          false )
    scopes

let has_atproto scopes = List.mem (Static Atproto) scopes

let has_transition_email scopes = List.mem (Static TransitionEmail) scopes

let has_transition_generic scopes = List.mem (Static TransitionGeneric) scopes

let has_transition_chat_bsky scopes =
  List.mem (Static TransitionChatBsky) scopes

module Transition = struct
  let allows_account scopes (opts : account_match) =
    if opts.attr = Email && opts.action == Read && has_transition_email scopes
    then true
    else allows_account scopes opts

  let allows_blob scopes (opts : blob_match) =
    if has_transition_generic scopes then true else allows_blob scopes opts

  let allows_repo scopes (opts : repo_match) =
    if has_transition_generic scopes then true else allows_repo scopes opts

  let allows_rpc scopes (opts : rpc_match) =
    if opts.lxm = "*" && has_transition_generic scopes then true
    else if
      (not (String.starts_with opts.lxm ~prefix:"chat.bsky."))
      && has_transition_generic scopes
    then true
    else if
      String.starts_with opts.lxm ~prefix:"chat.bsky."
      && has_transition_chat_bsky scopes
    then true
    else allows_rpc scopes opts
end

(* convert a permission from permission set to scope string *)
let permission_to_scope ~include_aud (perm : Lexicon_resolver.permission) =
  match perm.resource with
  | "rpc" -> (
    match perm.lxm with
    | None | Some [] ->
        None
    | Some lxms -> (
        let aud =
          match perm.aud with
          | Some a ->
              Some a
          | None ->
              if Option.value perm.inherit_aud ~default:false then include_aud
              else None
        in
        match aud with
        | None ->
            None (* rpc requires aud *)
        | Some a ->
            Some
              (List.map
                 (fun lxm ->
                   Printf.sprintf "rpc:%s?aud=%s" lxm (Uri.pct_encode a) )
                 lxms ) ) )
  | "repo" -> (
    match perm.collection with
    | None | Some [] ->
        None
    | Some collections ->
        let actions =
          Option.value perm.action ~default:["create"; "update"; "delete"]
        in
        let action_str =
          let action_set = List.sort String.compare actions in
          let default_set = ["create"; "delete"; "update"] in
          if action_set = default_set then ""
          else "?action=" ^ String.concat "," actions
        in
        Some
          (List.map
             (fun coll -> Printf.sprintf "repo:%s%s" coll action_str)
             collections ) )
  | "blob" -> (
    match perm.accept with
    | None | Some [] ->
        None
    | Some accepts ->
        Some (List.map (fun accept -> Printf.sprintf "blob:%s" accept) accepts)
    )
  | "account" | "identity" ->
      (* account and identity permissions can't be granted via permission sets *)
      None
  | _ ->
      None

(* expand include scope to list of granular scopes,
   validating authority for each permission nsid & applying inheritAud *)
let expand_include_scope (inc : include_scope)
    (ps : Lexicon_resolver.permission_set) =
  let allowed_resources = ["rpc"; "repo"] in
  ps.permissions
  |> List.filter (fun (p : Lexicon_resolver.permission) ->
      List.mem p.resource allowed_resources )
  |> List.filter_map (fun (p : Lexicon_resolver.permission) ->
      let nsids_to_check =
        match p.resource with
        | "rpc" ->
            Option.value p.lxm ~default:[]
        | "repo" ->
            (* filter out wildcards from collection validation *)
            Option.value p.collection ~default:[]
            |> List.filter (fun c -> c <> "*" && is_valid_nsid c)
        | _ ->
            []
      in
      let all_valid =
        List.for_all
          (fun nsid ->
            is_parent_authority_of ~include_nsid:inc.nsid ~permission_nsid:nsid )
          nsids_to_check
      in
      if all_valid then permission_to_scope ~include_aud:inc.aud p else None )
  |> List.flatten

(* expand all scopes, resolving includes, to expanded scope string *)
let expand_scopes (scopes : scope list) : string list Lwt.t =
  let%lwt expanded =
    Lwt_list.map_p
      (fun scope ->
        match scope with
        | Include inc -> (
          match%lwt Lexicon_resolver.resolve inc.nsid with
          | Error e ->
              Logs.warn (fun l ->
                  l "failed to resolve permission set %s: %s" inc.nsid e ) ;
              Lwt.return []
          | Ok ps ->
              Lwt.return (expand_include_scope inc ps) )
        | Static Atproto ->
            Lwt.return ["atproto"]
        | Static TransitionEmail ->
            Lwt.return ["transition:email"]
        | Static TransitionGeneric ->
            Lwt.return ["transition:generic"]
        | Static TransitionChatBsky ->
            Lwt.return ["transition:chat.bsky"]
        | Account perm ->
            let attr_str =
              match perm.attr with Email -> "email" | Repo -> "repo"
            in
            let actions_str =
              if List.mem Manage perm.actions then "?action=manage" else ""
            in
            Lwt.return [Printf.sprintf "account:%s%s" attr_str actions_str]
        | Identity perm ->
            let attr_str =
              match perm.attr with Handle -> "handle" | Any -> "*"
            in
            Lwt.return [Printf.sprintf "identity:%s" attr_str]
        | Repo perm ->
            let colls =
              List.map
                (function All -> "*" | Collection c -> c)
                perm.collections
            in
            let actions = List.map show_repo_action perm.actions in
            let action_str =
              if actions = ["create"; "update"; "delete"] then ""
              else "?action=" ^ String.concat "," actions
            in
            Lwt.return
              (List.map
                 (fun c -> Printf.sprintf "repo:%s%s" c action_str)
                 colls )
        | Rpc perm ->
            let lxms =
              List.map (function AnyLxm -> "*" | Lxm l -> l) perm.lxm
            in
            let aud_str = match perm.aud with AnyAud -> "*" | Aud a -> a in
            Lwt.return
              (List.map
                 (fun l ->
                   Printf.sprintf "rpc:%s?aud=%s" l (Uri.pct_encode aud_str) )
                 lxms )
        | Blob perm ->
            let accepts =
              List.map
                (function
                  | AnyMime ->
                      "*/*"
                  | TypeWildcard t ->
                      t ^ "/*"
                  | ExactMime (t, s) ->
                      t ^ "/" ^ s )
                perm.accept
            in
            Lwt.return (List.map (fun a -> Printf.sprintf "blob:%s" a) accepts) )
      scopes
  in
  Lwt.return (List.flatten expanded |> List.sort_uniq String.compare)

let scopes_to_string scopes = String.concat " " scopes
