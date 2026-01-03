(* generated from com.atproto.admin.sendEmail *)

(** Send email to a user's account email address. *)
module Main = struct
  let nsid = "com.atproto.admin.sendEmail"

  type params = unit
  let params_to_yojson () = `Assoc []

  type input =
    {
      recipient_did: string [@key "recipientDid"];
      content: string;
      subject: string option [@default None];
      sender_did: string [@key "senderDid"];
      comment: string option [@default None];
    }
  [@@deriving yojson {strict= false}]

  type output =
  {
    sent: bool;
  }
[@@deriving yojson {strict= false}]

  let call
      ~recipient_did
      ~content
      ?subject
      ~sender_did
      ?comment
      (client : Hermes.client) : output Lwt.t =
    let params = () in
    let input = Some ({recipient_did; content; subject; sender_did; comment} |> input_to_yojson) in
    Hermes.procedure client nsid (params_to_yojson params) input output_of_yojson
end

