let query_int_of_yojson = function
  | `Int n ->
      Ok n
  | `Intlit s | `String s -> (
    match int_of_string_opt s with
    | Some n ->
        Ok n
    | None ->
        Error "expected integer" )
  | _ ->
      Error "expected integer"

let query_int_option_of_yojson = function
  | `Null ->
      Ok None
  | j ->
      Result.map Option.some (query_int_of_yojson j)

let query_bool_of_yojson = function
  | `Bool b ->
      Ok b
  | `String "true" ->
      Ok true
  | `String "false" ->
      Ok false
  | _ ->
      Error "expected boolean"

let query_bool_option_of_yojson = function
  | `Null ->
      Ok None
  | j ->
      Result.map Option.some (query_bool_of_yojson j)

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
      Ok (List.filter_map (fun j -> Result.to_option (query_int_of_yojson j)) l)
  | `Null ->
      Ok []
  | j ->
      Result.map (fun i -> [i]) (query_int_of_yojson j)

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
  | `Null ->
      Ok None
  | j ->
      Result.map Option.some (query_int_list_of_yojson j)

let query_int_list_option_to_yojson = function
  | Some l ->
      `List (List.map (fun i -> `Int i) l)
  | None ->
      `Null
