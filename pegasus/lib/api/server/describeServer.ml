let handler : Dream.handler =
 fun _ ->
  let env = Env.load () in
  Dream.json
  @@ Format.sprintf
       {|{
  "did": "did:web:%s",
  "availableUserDomains": [".%s"],
  "inviteCodeRequired": %b,
  "links": {},
  "contact": {}
}|}
       env.hostname env.hostname env.invite_required
