[@@@warning "-missing-record-field-pattern"]

open Types

let insert_par_request conn req =
  Util.use_pool conn
  @@ [%rapper
       execute
         {sql|
        INSERT INTO oauth_requests (request_id, client_id, request_data, dpop_jkt, expires_at, created_at)
        VALUES (%string{request_id}, %string{client_id}, %string{request_data}, %string?{dpop_jkt}, %int{expires_at}, %int{created_at})
      |sql}
         record_in]
       req

let get_par_request conn request_id =
  Util.use_pool conn
  @@ [%rapper
       get_opt
         {sql|
        SELECT @string{request_id}, @string{client_id}, @string{request_data},
               @string?{dpop_jkt}, @int{expires_at}, @int{created_at}
        FROM oauth_requests
        WHERE request_id = %string{request_id}
        AND expires_at > %int{now}
      |sql}
         record_out]
       ~request_id ~now:(Util.now_ms ())

let insert_auth_code conn code =
  Util.use_pool conn
  @@ [%rapper
       execute
         {sql|
        INSERT INTO oauth_codes (code, request_id, authorized_by, authorized_at, expires_at, used)
        VALUES (%string{code}, %string{request_id}, %string?{authorized_by}, %int?{authorized_at}, %int{expires_at}, 0)
      |sql}
         record_in]
       code

let get_auth_code conn code =
  Util.use_pool conn
  @@ [%rapper
       get_opt
         {sql|
        SELECT @string{code}, @string{request_id}, @string?{authorized_by},
               @int?{authorized_at}, @int{expires_at}, @bool{used}
        FROM oauth_codes
        WHERE code = %string{code}
      |sql}
         record_out]
       ~code

let activate_auth_code conn code did =
  let authorized_at = Util.now_ms () in
  Util.use_pool conn
  @@ [%rapper
       execute
         {sql|
        UPDATE oauth_codes
        SET authorized_by = %string{did},
            authorized_at = %int{authorized_at}
        WHERE code = %string{code}
      |sql}]
       ~did ~authorized_at ~code

let consume_auth_code conn code =
  Util.use_pool conn
  @@ [%rapper
       get_opt
         {sql|
        UPDATE oauth_codes
        SET used = 1
        WHERE code = %string{code} AND used = 0
        RETURNING @string{code}, @string{request_id}, @string?{authorized_by},
                  @int?{authorized_at}, @int{expires_at}, @bool{used}
      |sql}
         record_out]
       ~code

let insert_oauth_token conn token =
  Util.use_pool conn
  @@ [%rapper
       execute
         {sql|
         INSERT INTO oauth_tokens (refresh_token, client_id, did, dpop_jkt, scope, created_at, expires_at, last_refreshed_at, last_ip, last_user_agent)
         VALUES (%string{refresh_token}, %string{client_id}, %string{did}, %string{dpop_jkt}, %string{scope}, %int{created_at}, %int{expires_at}, %int{last_refreshed_at}, %string{last_ip}, %string?{last_user_agent})
      |sql}
         record_in]
       token

let get_oauth_token_by_refresh conn refresh_token =
  Util.use_pool conn
  @@ [%rapper
       get_opt
         {sql|
        SELECT @string{refresh_token}, @string{client_id}, @string{did},
               @string{dpop_jkt}, @string{scope}, @int{created_at}, @int{expires_at},
               @int{last_refreshed_at}, @string{last_ip}, @string?{last_user_agent}
        FROM oauth_tokens
        WHERE refresh_token = %string{refresh_token}
      |sql}
         record_out]
       ~refresh_token

let update_oauth_token conn ~old_refresh_token ~new_refresh_token ~expires_at
    ~ip ~user_agent =
  Util.use_pool conn
  @@ [%rapper
       execute
         {sql|
        UPDATE oauth_tokens
        SET refresh_token = %string{new_refresh_token},
            expires_at = %int{expires_at}, last_ip = %string{ip},
            last_user_agent = %string?{user_agent}
        WHERE refresh_token = %string{old_refresh_token}
      |sql}]
       ~new_refresh_token ~expires_at ~old_refresh_token ~ip ~user_agent

let delete_oauth_token_by_refresh conn refresh_token =
  Util.use_pool conn
  @@ [%rapper
       execute
         {sql|
        DELETE FROM oauth_tokens WHERE refresh_token = %string{refresh_token}
      |sql}]
       ~refresh_token

let get_oauth_tokens_by_did conn did =
  Util.use_pool conn
  @@ [%rapper
       get_many
         {sql|
        SELECT @string{refresh_token}, @string{client_id}, @string{did},
                @string{dpop_jkt}, @string{scope}, @int{created_at}, @int{expires_at},
                @int{last_refreshed_at}, @string{last_ip}, @string?{last_user_agent}
        FROM oauth_tokens
        WHERE did = %string{did}
        ORDER BY expires_at ASC
      |sql}
         record_out]
       ~did

let get_distinct_clients_by_did conn did =
  Util.use_pool conn
  @@ [%rapper
       get_many
         {sql|
        SELECT DISTINCT @string{client_id}, MAX(@int{last_refreshed_at}) as last_refreshed_at
        FROM oauth_tokens
        WHERE did = %string{did}
        GROUP BY client_id
        ORDER BY last_refreshed_at DESC
      |sql}]
       ~did

let get_distinct_devices_by_did conn did =
  Util.use_pool conn
  @@ [%rapper
       get_many
         {sql|
        SELECT @string{last_ip}, @string?{last_user_agent},
               MAX(@int{last_refreshed_at}) as last_refreshed_at
        FROM oauth_tokens
        WHERE did = %string{did}
        GROUP BY last_ip, last_user_agent
        ORDER BY last_refreshed_at DESC
      |sql}]
       ~did

let delete_oauth_tokens_by_client conn ~did ~client_id =
  Util.use_pool conn
  @@ [%rapper
       execute
         {sql|
        DELETE FROM oauth_tokens
        WHERE did = %string{did} AND client_id = %string{client_id}
      |sql}]
       ~did ~client_id

let delete_oauth_tokens_by_device conn ~did ~last_ip ~last_user_agent =
  Util.use_pool conn
  @@ [%rapper
       execute
         {sql|
        DELETE FROM oauth_tokens
        WHERE did = %string{did} AND last_ip = %string{last_ip}
          AND (last_user_agent = %string?{last_user_agent} OR (last_user_agent IS NULL AND %string?{last_user_agent} IS NULL))
      |sql}]
       ~did ~last_ip ~last_user_agent
