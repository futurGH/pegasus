let handler : Dream.handler =
 fun _ ->
  Dream.json
  @@ Format.sprintf
       {|{
  "did": "did:web:%s",
  "availableUserDomains": [".%s"],
  "inviteCodeRequired": %b,
  "links": {},
  "contact": {}
}|}
       Env.hostname Env.hostname Env.invite_required
