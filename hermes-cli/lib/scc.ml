open Lexicon_types

(** returns SCCs in reverse topological order (dependencies first)
    each SCC is a list of nodes *)
let find_sccs (type node) (nodes : node list) ~(get_id : node -> string)
    ~(get_deps : node -> string list) : node list list =
  (* build node map: id -> node *)
  let node_map =
    List.fold_left (fun m node -> (get_id node, node) :: m) [] nodes
  in
  let node_ids = List.map get_id nodes in
  (* build dependency map *)
  let deps = List.map (fun node -> (get_id node, get_deps node)) nodes in
  (* Tarjan's algorithm state *)
  let index_counter = ref 0 in
  let indices = Hashtbl.create 64 in
  let lowlinks = Hashtbl.create 64 in
  let on_stack = Hashtbl.create 64 in
  let stack = ref [] in
  let sccs = ref [] in
  let rec strongconnect id =
    let index = !index_counter in
    incr index_counter ;
    Hashtbl.add indices id index ;
    Hashtbl.add lowlinks id index ;
    Hashtbl.add on_stack id true ;
    stack := id :: !stack ;
    (* visit successors *)
    let successors =
      try List.assoc id deps |> List.filter (fun s -> List.mem s node_ids)
      with Not_found -> []
    in
    List.iter
      (fun succ ->
        if not (Hashtbl.mem indices succ) then begin
          (* successor not yet visited *)
          strongconnect succ ;
          Hashtbl.replace lowlinks id
            (min (Hashtbl.find lowlinks id) (Hashtbl.find lowlinks succ))
        end
        else if Hashtbl.find_opt on_stack succ = Some true then
          (* successor is on stack, part of current SCC *)
          Hashtbl.replace lowlinks id
            (min (Hashtbl.find lowlinks id) (Hashtbl.find indices succ)) )
      successors ;
    (* if this is a root node, pop the SCC *)
    if Hashtbl.find lowlinks id = Hashtbl.find indices id then begin
      let rec pop_scc acc =
        match !stack with
        | [] ->
            acc
        | top :: rest ->
            stack := rest ;
            Hashtbl.replace on_stack top false ;
            if top = id then top :: acc else pop_scc (top :: acc)
      in
      let scc_ids = pop_scc [] in
      (* convert IDs to nodes, preserving original order *)
      let scc_nodes =
        List.filter_map
          (fun n -> List.assoc_opt n node_map)
          (List.filter (fun n -> List.mem n scc_ids) node_ids)
      in
      if scc_nodes <> [] then sccs := scc_nodes :: !sccs
    end
  in
  (* run on all nodes *)
  List.iter
    (fun id -> if not (Hashtbl.mem indices id) then strongconnect id)
    node_ids ;
  (* SCCs are prepended, so reverse to get topological order *)
  List.rev !sccs

(** returns list of definition names that this type depends on within the same nsid *)
let rec collect_local_refs nsid acc = function
  | Array {items; _} ->
      collect_local_refs nsid acc items
  | Ref {ref_; _} ->
      if String.length ref_ > 0 && ref_.[0] = '#' then
        (* local ref: #foo *)
        let def_name = String.sub ref_ 1 (String.length ref_ - 1) in
        def_name :: acc
      else begin
        (* check if it's a self-reference: nsid#foo *)
        match String.split_on_char '#' ref_ with
        | [ext_nsid; def_name] when ext_nsid = nsid ->
            def_name :: acc
        | _ ->
            acc
      end
  | Union {refs; _} ->
      List.fold_left
        (fun a r ->
          if String.length r > 0 && r.[0] = '#' then
            let def_name = String.sub r 1 (String.length r - 1) in
            def_name :: a
          else
            match String.split_on_char '#' r with
            | [ext_nsid; def_name] when ext_nsid = nsid ->
                def_name :: a
            | _ ->
                a )
        acc refs
  | Object {properties; _} ->
      List.fold_left
        (fun a (_, (prop : property)) -> collect_local_refs nsid a prop.type_def)
        acc properties
  | Record {record; _} ->
      List.fold_left
        (fun a (_, (prop : property)) -> collect_local_refs nsid a prop.type_def)
        acc record.properties
  | Query {parameters; output; _} -> (
      let acc =
        match parameters with
        | Some params ->
            List.fold_left
              (fun a (_, (prop : property)) ->
                collect_local_refs nsid a prop.type_def )
              acc params.properties
        | None ->
            acc
      in
      match output with
      | Some body ->
          Option.fold ~none:acc ~some:(collect_local_refs nsid acc) body.schema
      | None ->
          acc )
  | Procedure {parameters; input; output; _} -> (
      let acc =
        match parameters with
        | Some params ->
            List.fold_left
              (fun a (_, (prop : property)) ->
                collect_local_refs nsid a prop.type_def )
              acc params.properties
        | None ->
            acc
      in
      let acc =
        match input with
        | Some body ->
            Option.fold ~none:acc
              ~some:(collect_local_refs nsid acc)
              body.schema
        | None ->
            acc
      in
      match output with
      | Some body ->
          Option.fold ~none:acc ~some:(collect_local_refs nsid acc) body.schema
      | None ->
          acc )
  | _ ->
      acc

(** find SCCs among definitions within a single lexicon
    returns SCCs in reverse topological order *)
let find_def_sccs nsid (defs : def_entry list) : def_entry list list =
  find_sccs defs
    ~get_id:(fun def -> def.name)
    ~get_deps:(fun def -> collect_local_refs nsid [] def.type_def)

(** get external nsid dependencies for a lexicon *)
let get_external_nsids (doc : lexicon_doc) : string list =
  let nsids = ref [] in
  let add_nsid s = if not (List.mem s !nsids) then nsids := s :: !nsids in
  let rec collect_from_type = function
    | Array {items; _} ->
        collect_from_type items
    | Ref {ref_; _} ->
        if String.length ref_ > 0 && ref_.[0] <> '#' then begin
          match String.split_on_char '#' ref_ with
          | ext_nsid :: _ ->
              add_nsid ext_nsid
          | [] ->
              ()
        end
    | Union {refs; _} ->
        List.iter
          (fun r ->
            if String.length r > 0 && r.[0] <> '#' then
              match String.split_on_char '#' r with
              | ext_nsid :: _ ->
                  add_nsid ext_nsid
              | [] ->
                  () )
          refs
    | Object {properties; _} ->
        List.iter
          (fun (_, (prop : property)) -> collect_from_type prop.type_def)
          properties
    | Query {parameters; output; _} ->
        Option.iter
          (fun p ->
            List.iter
              (fun (_, (prop : property)) -> collect_from_type prop.type_def)
              p.properties )
          parameters ;
        Option.iter (fun o -> Option.iter collect_from_type o.schema) output
    | Procedure {parameters; input; output; _} ->
        Option.iter
          (fun p ->
            List.iter
              (fun (_, (prop : property)) -> collect_from_type prop.type_def)
              p.properties )
          parameters ;
        Option.iter (fun i -> Option.iter collect_from_type i.schema) input ;
        Option.iter (fun o -> Option.iter collect_from_type o.schema) output
    | Record {record; _} ->
        List.iter
          (fun (_, (prop : property)) -> collect_from_type prop.type_def)
          record.properties
    | _ ->
        ()
  in
  List.iter (fun def -> collect_from_type def.type_def) doc.defs ;
  !nsids

(** find SCCs between lexicon files, in reverse topological order *)
let find_file_sccs (lexicons : lexicon_doc list) : lexicon_doc list list =
  let nsids = List.map (fun doc -> doc.id) lexicons in
  find_sccs lexicons
    ~get_id:(fun doc -> doc.id)
    ~get_deps:(fun doc ->
      (* filter to only include nsids we have *)
      get_external_nsids doc |> List.filter (fun n -> List.mem n nsids) )
