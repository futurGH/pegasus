  type string_or_null = string option

  let string_or_null_to_yojson = function Some s -> `String s | None -> `Null

  let string_or_null_of_yojson = function
    | `String s ->
        Ok (Some s)
    | `Null ->
        Ok None
    | _ ->
        Error "invalid field value"

  type string_or_strings = [`String of string | `Strings of string list]

  let string_or_strings_to_yojson = function
    | `String c ->
        `String c
    | `Strings cs ->
        `List (List.map (fun c -> `String c) cs)

  let string_or_strings_of_yojson = function
    | `String c ->
        Ok (`Strings [c])
    | `List cs ->
        Ok (`Strings (Yojson.Safe.Util.filter_string cs))
    | _ ->
        Error "invalid field value"

  type string_map = (string * string) list

  let string_map_to_yojson = function
    | [] ->
        `Assoc []
    | m ->
        `Assoc (List.map (fun (k, v) -> (k, `String v)) m)

  let string_map_of_yojson = function
    | `Null ->
        Ok []
    | `Assoc m ->
        Ok
          (List.filter_map
             (fun (k, v) ->
               match (k, v) with _, `String s -> Some (k, s) | _, _ -> None )
             m )
    | _ ->
        Error "invalid field value"

  type string_or_string_map = [`String of string | `String_map of string_map]

  let string_or_string_map_to_yojson = function
    | `String c ->
        `String c
    | `String_map m ->
        `Assoc (List.map (fun (k, v) -> (k, `String v)) m)

  let string_or_string_map_of_yojson = function
    | `String c ->
        Ok (`String c)
    | `Assoc m ->
        string_map_of_yojson (`Assoc m) |> Result.map (fun m -> `String_map m)
    | _ ->
        Error "invalid field value"

  type string_or_string_map_or_either_list =
    [ `String of string
    | `String_map of string_map
    | `List of string_or_string_map list ]

  let string_or_string_map_or_either_list_to_yojson = function
    | `String c ->
        `String c
    | `String_map m ->
        `Assoc (List.map (fun (k, v) -> (k, `String v)) m)
    | `List l ->
        `List (List.map string_or_string_map_to_yojson l)

  let string_or_string_map_or_either_list_of_yojson = function
    | `String c ->
        Ok (`String c)
    | `Assoc m ->
        string_map_of_yojson (`Assoc m) |> Result.map (fun m -> `String_map m)
    | `List l ->
        Ok
          (`List
             ( List.map string_or_string_map_of_yojson l
             |> List.filter_map (function Ok x -> Some x | Error _ -> None) ) )
    | _ ->
        Error "invalid field value"
