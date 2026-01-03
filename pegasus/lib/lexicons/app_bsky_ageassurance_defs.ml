(* generated from app.bsky.ageassurance.defs *)

(** string type with known values: The access level granted based on Age Assurance data we've processed. *)
type access = string
let access_of_yojson = function
  | `String s -> Ok s
  | _ -> Error "access: expected string"
let access_to_yojson s = `String s

(** string type with known values: The status of the Age Assurance process. *)
type status = string
let status_of_yojson = function
  | `String s -> Ok s
  | _ -> Error "status: expected string"
let status_to_yojson s = `String s

type state =
  {
    last_initiated_at: string option [@key "lastInitiatedAt"] [@default None];
    status: status;
    access: access;
  }
[@@deriving yojson {strict= false}]

type state_metadata =
  {
    account_created_at: string option [@key "accountCreatedAt"] [@default None];
  }
[@@deriving yojson {strict= false}]

type config_region_rule_if_account_older_than =
  {
    date: string;
    access: access;
  }
[@@deriving yojson {strict= false}]

type config_region_rule_if_account_newer_than =
  {
    date: string;
    access: access;
  }
[@@deriving yojson {strict= false}]

type config_region_rule_if_assured_under_age =
  {
    age: int;
    access: access;
  }
[@@deriving yojson {strict= false}]

type config_region_rule_if_assured_over_age =
  {
    age: int;
    access: access;
  }
[@@deriving yojson {strict= false}]

type config_region_rule_if_declared_under_age =
  {
    age: int;
    access: access;
  }
[@@deriving yojson {strict= false}]

type config_region_rule_if_declared_over_age =
  {
    age: int;
    access: access;
  }
[@@deriving yojson {strict= false}]

type config_region_rule_default =
  {
    access: access;
  }
[@@deriving yojson {strict= false}]

type rules_item =
  | ConfigRegionRuleDefault of config_region_rule_default
  | ConfigRegionRuleIfDeclaredOverAge of config_region_rule_if_declared_over_age
  | ConfigRegionRuleIfDeclaredUnderAge of config_region_rule_if_declared_under_age
  | ConfigRegionRuleIfAssuredOverAge of config_region_rule_if_assured_over_age
  | ConfigRegionRuleIfAssuredUnderAge of config_region_rule_if_assured_under_age
  | ConfigRegionRuleIfAccountNewerThan of config_region_rule_if_account_newer_than
  | ConfigRegionRuleIfAccountOlderThan of config_region_rule_if_account_older_than
  | Unknown of Yojson.Safe.t

let rules_item_of_yojson json =
  let open Yojson.Safe.Util in
  try
    match json |> member "$type" |> to_string with
    | "app.bsky.ageassurance.defs#configRegionRuleDefault" ->
        (match config_region_rule_default_of_yojson json with
         | Ok v -> Ok (ConfigRegionRuleDefault v)
         | Error e -> Error e)
    | "app.bsky.ageassurance.defs#configRegionRuleIfDeclaredOverAge" ->
        (match config_region_rule_if_declared_over_age_of_yojson json with
         | Ok v -> Ok (ConfigRegionRuleIfDeclaredOverAge v)
         | Error e -> Error e)
    | "app.bsky.ageassurance.defs#configRegionRuleIfDeclaredUnderAge" ->
        (match config_region_rule_if_declared_under_age_of_yojson json with
         | Ok v -> Ok (ConfigRegionRuleIfDeclaredUnderAge v)
         | Error e -> Error e)
    | "app.bsky.ageassurance.defs#configRegionRuleIfAssuredOverAge" ->
        (match config_region_rule_if_assured_over_age_of_yojson json with
         | Ok v -> Ok (ConfigRegionRuleIfAssuredOverAge v)
         | Error e -> Error e)
    | "app.bsky.ageassurance.defs#configRegionRuleIfAssuredUnderAge" ->
        (match config_region_rule_if_assured_under_age_of_yojson json with
         | Ok v -> Ok (ConfigRegionRuleIfAssuredUnderAge v)
         | Error e -> Error e)
    | "app.bsky.ageassurance.defs#configRegionRuleIfAccountNewerThan" ->
        (match config_region_rule_if_account_newer_than_of_yojson json with
         | Ok v -> Ok (ConfigRegionRuleIfAccountNewerThan v)
         | Error e -> Error e)
    | "app.bsky.ageassurance.defs#configRegionRuleIfAccountOlderThan" ->
        (match config_region_rule_if_account_older_than_of_yojson json with
         | Ok v -> Ok (ConfigRegionRuleIfAccountOlderThan v)
         | Error e -> Error e)
    | _ -> Ok (Unknown json)
  with _ -> Error "failed to parse union"

let rules_item_to_yojson = function
  | ConfigRegionRuleDefault v ->
      (match config_region_rule_default_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.ageassurance.defs#configRegionRuleDefault") :: fields)
       | other -> other)
  | ConfigRegionRuleIfDeclaredOverAge v ->
      (match config_region_rule_if_declared_over_age_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.ageassurance.defs#configRegionRuleIfDeclaredOverAge") :: fields)
       | other -> other)
  | ConfigRegionRuleIfDeclaredUnderAge v ->
      (match config_region_rule_if_declared_under_age_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.ageassurance.defs#configRegionRuleIfDeclaredUnderAge") :: fields)
       | other -> other)
  | ConfigRegionRuleIfAssuredOverAge v ->
      (match config_region_rule_if_assured_over_age_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.ageassurance.defs#configRegionRuleIfAssuredOverAge") :: fields)
       | other -> other)
  | ConfigRegionRuleIfAssuredUnderAge v ->
      (match config_region_rule_if_assured_under_age_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.ageassurance.defs#configRegionRuleIfAssuredUnderAge") :: fields)
       | other -> other)
  | ConfigRegionRuleIfAccountNewerThan v ->
      (match config_region_rule_if_account_newer_than_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.ageassurance.defs#configRegionRuleIfAccountNewerThan") :: fields)
       | other -> other)
  | ConfigRegionRuleIfAccountOlderThan v ->
      (match config_region_rule_if_account_older_than_to_yojson v with
       | `Assoc fields -> `Assoc (("$type", `String "app.bsky.ageassurance.defs#configRegionRuleIfAccountOlderThan") :: fields)
       | other -> other)
  | Unknown j -> j

type config_region =
  {
    country_code: string [@key "countryCode"];
    region_code: string option [@key "regionCode"] [@default None];
    rules: rules_item list;
  }
[@@deriving yojson {strict= false}]

type config =
  {
    regions: config_region list;
  }
[@@deriving yojson {strict= false}]

type event =
  {
    created_at: string [@key "createdAt"];
    attempt_id: string [@key "attemptId"];
    status: string;
    access: string;
    country_code: string [@key "countryCode"];
    region_code: string option [@key "regionCode"] [@default None];
    email: string option [@default None];
    init_ip: string option [@key "initIp"] [@default None];
    init_ua: string option [@key "initUa"] [@default None];
    complete_ip: string option [@key "completeIp"] [@default None];
    complete_ua: string option [@key "completeUa"] [@default None];
  }
[@@deriving yojson {strict= false}]

