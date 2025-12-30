(* generated from com.atproto.admin.defs *)

type status_attr =
  {
    applied: bool;
    ref: string option [@default None];
  }
[@@deriving yojson {strict= false}]

type threat_signature =
  {
    property: string;
    value: string;
  }
[@@deriving yojson {strict= false}]

type account_view =
  {
    did: string;
    handle: string;
    email: string option [@default None];
    related_records: Yojson.Safe.t list option [@key "relatedRecords"] [@default None];
    indexed_at: string [@key "indexedAt"];
    invited_by: Com_atproto_server_defs.invite_code option [@key "invitedBy"] [@default None];
    invites: Com_atproto_server_defs.invite_code list option [@default None];
    invites_disabled: bool option [@key "invitesDisabled"] [@default None];
    email_confirmed_at: string option [@key "emailConfirmedAt"] [@default None];
    invite_note: string option [@key "inviteNote"] [@default None];
    deactivated_at: string option [@key "deactivatedAt"] [@default None];
    threat_signatures: threat_signature list option [@key "threatSignatures"] [@default None];
  }
[@@deriving yojson {strict= false}]

type repo_ref =
  {
    did: string;
  }
[@@deriving yojson {strict= false}]

type repo_blob_ref =
  {
    did: string;
    cid: string;
    record_uri: string option [@key "recordUri"] [@default None];
  }
[@@deriving yojson {strict= false}]

