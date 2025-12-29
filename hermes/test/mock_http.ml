(** mock HTTP backend for testing *)

open Lwt.Syntax

type request =
  { meth: [`GET | `POST]
  ; uri: Uri.t
  ; headers: Cohttp.Header.t
  ; body: string option }

type response =
  { status: Cohttp.Code.status_code
  ; headers: (string * string) list
  ; body: string }

type handler = request -> response Lwt.t

let make_cohttp_response (r : response) : Hermes.Http_backend.response =
  let resp =
    Cohttp.Response.make ~status:r.status
      ~headers:(Cohttp.Header.of_list r.headers)
      ()
  in
  let body = Cohttp_lwt.Body.of_string r.body in
  (resp, body)

(** create a mock HTTP backend with a handler *)
module Make (Config : sig
  val handler : handler ref
end) : Hermes.Http_backend.S = struct
  let get ~headers uri =
    let req = {meth= `GET; uri; headers; body= None} in
    let* r = !Config.handler req in
    Lwt.return (make_cohttp_response r)

  let post ~headers ~body uri =
    let* body_str = Cohttp_lwt.Body.to_string body in
    let req = {meth= `POST; uri; headers; body= Some body_str} in
    let* r = !Config.handler req in
    Lwt.return (make_cohttp_response r)
end

(** simple response builders *)

let json_response ?(status = `OK) ?(headers = []) json =
  { status
  ; headers= ("content-type", "application/json") :: headers
  ; body= Yojson.Safe.to_string json }

let bytes_response ?(status = `OK) ~content_type body =
  {status; headers= [("content-type", content_type)]; body}

let error_response ~status ~error ?message () =
  let msg_field =
    match message with Some m -> [("message", `String m)] | None -> []
  in
  json_response ~status (`Assoc ([("error", `String error)] @ msg_field))

let empty_response ?(status = `OK) () = {status; headers= []; body= ""}

(** queue-based mock, returns responses in order *)
module Queue = struct
  type t = {mutable responses: response list; mutable requests: request list}

  let create responses = {responses; requests= []}

  let handler q req =
    q.requests <- q.requests @ [req] ;
    match q.responses with
    | [] ->
        failwith "Mock_http.Queue: no more responses"
    | r :: rest ->
        q.responses <- rest ;
        Lwt.return r

  let get_requests q = q.requests

  let clear q =
    q.responses <- [] ;
    q.requests <- []
end

(** pattern-matching mock, selects responses based on request *)
module Pattern = struct
  type rule =
    { nsid: string option  (** match NSID in path *)
    ; meth: [`GET | `POST] option  (** match method *)
    ; response: response }

  type t =
    {rules: rule list; mutable requests: request list; default: response option}

  let create ?(default = None) rules = {rules; requests= []; default}

  let extract_nsid uri =
    let path = Uri.path uri in
    if String.length path > 6 && String.sub path 0 6 = "/xrpc/" then
      Some (String.sub path 6 (String.length path - 6))
    else None

  let matches rule req =
    let nsid_matches =
      match rule.nsid with
      | None ->
          true
      | Some nsid ->
          extract_nsid req.uri = Some nsid
    in
    let meth_matches =
      match rule.meth with None -> true | Some m -> req.meth = m
    in
    nsid_matches && meth_matches

  let handler t req =
    t.requests <- t.requests @ [req] ;
    match List.find_opt (fun r -> matches r req) t.rules with
    | Some rule ->
        Lwt.return rule.response
    | None -> (
      match t.default with
      | Some r ->
          Lwt.return r
      | None ->
          failwith
            ("Mock_http.Pattern: no matching rule for " ^ Uri.to_string req.uri)
      )

  let get_requests t = t.requests
end
