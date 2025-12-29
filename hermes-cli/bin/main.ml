open Hermes_cli

(* recursively find all json files in a directory *)
let find_json_files dir =
  let rec aux acc path =
    if Sys.is_directory path then
      Sys.readdir path |> Array.to_list
      |> List.map (Filename.concat path)
      |> List.fold_left aux acc
    else if Filename.check_suffix path ".json" then path :: acc
    else acc
  in
  aux [] dir

(* generate module structure from lexicons *)
let generate ~input_dir ~output_dir ~module_name =
  (* create output directory *)
  if not (Sys.file_exists output_dir) then Sys.mkdir output_dir 0o755 ;
  (* find all lexicon files *)
  let files = find_json_files input_dir in
  Printf.printf "Found %d lexicon files\n" (List.length files) ;
  (* parse all files *)
  let lexicons =
    List.filter_map
      (fun path ->
        match Parser.parse_file path with
        | Ok doc ->
            Printf.printf "  Parsed: %s\n" doc.Lexicon_types.id ;
            Some doc
        | Error e ->
            Printf.eprintf "  Error parsing %s: %s\n" path e ;
            None )
      files
  in
  Printf.printf "Successfully parsed %d lexicons\n" (List.length lexicons) ;
  (* group by namespace, all but last segment *)
  let by_namespace = Hashtbl.create 64 in
  List.iter
    (fun doc ->
      let segments = String.split_on_char '.' doc.Lexicon_types.id in
      match List.rev segments with
      | _last :: rest ->
          let ns = String.concat "." (List.rev rest) in
          let existing =
            try Hashtbl.find by_namespace ns with Not_found -> []
          in
          Hashtbl.replace by_namespace ns (doc :: existing)
      | [] ->
          () )
    lexicons ;
  (* generate file for each lexicon *)
  List.iter
    (fun doc ->
      let code = Codegen.gen_lexicon_module doc in
      let rel_path = Naming.file_path_of_nsid doc.Lexicon_types.id in
      let full_path = Filename.concat output_dir rel_path in
      (* write file *)
      let oc = open_out full_path in
      output_string oc code ;
      close_out oc ;
      Printf.printf "  Generated: %s\n" rel_path )
    lexicons ;
  (* generate index file *)
  let index_path =
    Filename.concat output_dir (String.lowercase_ascii module_name ^ ".ml")
  in
  let oc = open_out index_path in
  Printf.fprintf oc "(* %s - generated from atproto lexicons *)\n\n" module_name ;
  (* export each lexicon as a module alias *)
  List.iter
    (fun doc ->
      let flat_module = Naming.flat_module_name_of_nsid doc.Lexicon_types.id in
      Printf.fprintf oc "module %s = %s\n" flat_module flat_module )
    lexicons ;
  close_out oc ;
  Printf.printf "Generated index: %s\n" index_path ;
  (* generate dune file *)
  let dune_path = Filename.concat output_dir "dune" in
  let oc = open_out dune_path in
  Printf.fprintf oc "(library\n" ;
  Printf.fprintf oc " (name %s)\n" (String.lowercase_ascii module_name) ;
  Printf.fprintf oc " (libraries hermes yojson lwt)\n" ;
  Printf.fprintf oc " (preprocess (pps ppx_deriving_yojson)))\n" ;
  close_out oc ;
  Printf.printf "Generated dune file\n" ;
  Printf.printf "Done! Generated %d modules\n" (List.length lexicons)

let input_dir =
  let doc = "directory containing lexicon JSON files" in
  Cmdliner.Arg.(
    required & opt (some dir) None & info ["i"; "input"] ~docv:"DIR" ~doc )

let output_dir =
  let doc = "output directory for generated code" in
  Cmdliner.Arg.(
    required & opt (some string) None & info ["o"; "output"] ~docv:"DIR" ~doc )

let module_name =
  let doc = "name of the generated module" in
  Cmdliner.Arg.(
    value
    & opt string "Hermes_lexicons"
    & info ["m"; "module-name"] ~docv:"NAME" ~doc )

let generate_cmd =
  let doc = "generate ocaml types from atproto lexicons" in
  let info = Cmdliner.Cmd.info "generate" ~doc in
  let generate' input_dir output_dir module_name =
    generate ~input_dir ~output_dir ~module_name
  in
  Cmdliner.Cmd.v info
    Cmdliner.Term.(const generate' $ input_dir $ output_dir $ module_name)

let main_cmd =
  let doc = "hermes - atproto lexicon code generator" in
  let info = Cmdliner.Cmd.info "hermes-cli" ~version:"0.1.0" ~doc in
  Cmdliner.Cmd.group info [generate_cmd]

let () = exit (Cmdliner.Cmd.eval main_cmd)
