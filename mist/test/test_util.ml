open Mist.Util

let test_leading_zeros () =
  let cases = Hashtbl.create 4 in
  Hashtbl.add cases "2653ae71" 0 ;
  Hashtbl.add cases "blue" 1 ;
  Hashtbl.add cases "app.bsky.feed.post/454397e440ec" 4 ;
  Hashtbl.add cases "app.bsky.feed.post/9adeb165882c" 8 ;
  cases
  |> Hashtbl.iter (fun key value ->
         Alcotest.(check int)
           ("leading zeros on hash " ^ key)
           value
           (leading_zeros_on_hash key) )

let test_shared_prefix_length () =
  let cases = Hashtbl.create 5 in
  Hashtbl.add cases ("2653ae71", "2653ae71") 8 ;
  Hashtbl.add cases ("2653ae71", "2653ae01") 6 ;
  Hashtbl.add cases ("2653ae71", "26530e71") 4 ;
  Hashtbl.add cases ("2653ae71", "2603ae71") 2 ;
  Hashtbl.add cases ("2653ae71", "0653ae71") 0 ;
  cases
  |> Hashtbl.iter (fun (a, b) value ->
         Alcotest.(check int)
           ("prefix length between " ^ a ^ " and " ^ b)
           value (shared_prefix_length a b) )

let () =
  Alcotest.run "util"
    [ ( "repo utils"
      , [ ("leading_zeros", `Quick, test_leading_zeros)
        ; ("shared_prefix_length", `Quick, test_shared_prefix_length) ] ) ]
