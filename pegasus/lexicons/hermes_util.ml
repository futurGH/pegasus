(* Lexicons - generated from atproto lexicons *)

let query_string_list_of_yojson = function
  | `List l ->
      Ok (List.filter_map (function `String s -> Some s | _ -> None) l)
  | `String s ->
      Ok [s]
  | `Null ->
      Ok []
  | _ ->
      Error "expected string or string list"

let query_string_list_to_yojson l = `List (List.map (fun s -> `String s) l)

let query_int_list_of_yojson = function
  | `List l ->
      Ok (List.filter_map (function `Int i -> Some i | _ -> None) l)
  | `Int i ->
      Ok [i]
  | `Null ->
      Ok []
  | _ ->
      Error "expected int or int list"

let query_int_list_to_yojson l = `List (List.map (fun i -> `Int i) l)

let query_string_list_option_of_yojson = function
  | `List l ->
      Ok (Some (List.filter_map (function `String s -> Some s | _ -> None) l))
  | `String s ->
      Ok (Some [s])
  | `Null ->
      Ok None
  | _ ->
      Error "expected string or string list"

let query_string_list_option_to_yojson = function
  | Some l ->
      `List (List.map (fun s -> `String s) l)
  | None ->
      `Null

let query_int_list_option_of_yojson = function
  | `List l ->
      Ok (Some (List.filter_map (function `Int i -> Some i | _ -> None) l))
  | `Int i ->
      Ok (Some [i])
  | `Null ->
      Ok None
  | _ ->
      Error "expected int or int list"

let query_int_list_option_to_yojson = function
  | Some l ->
      `List (List.map (fun i -> `Int i) l)
  | None ->
      `Null
