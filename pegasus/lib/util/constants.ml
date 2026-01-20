  let data_dir =
    Core.Filename.to_absolute_exn Env.data_dir
      ~relative_to:(Core_unix.getcwd ())

  let pegasus_db_filepath = Filename.concat data_dir "pegasus.db"

  let pegasus_db_location = "sqlite3://" ^ pegasus_db_filepath |> Uri.of_string

  let user_db_filepath did =
    let dirname = Filename.concat data_dir "store" in
    let filename = Str.global_replace (Str.regexp ":") "_" did in
    Filename.concat dirname filename ^ ".db"

  let user_db_location did =
    "sqlite3://" ^ user_db_filepath did |> Uri.of_string

  let user_blobs_location did =
    did
    |> Str.global_replace (Str.regexp ":") "_"
    |> (Filename.concat data_dir "blobs" |> Filename.concat)
