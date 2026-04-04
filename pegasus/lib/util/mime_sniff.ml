let matches_at data offset pattern =
  let plen = String.length pattern in
  if Bytes.length data < offset + plen then false
  else
    let rec loop i =
      if i >= plen then true
      else if Bytes.get_uint8 data (offset + i) <> Char.code pattern.[i] then
        false
      else loop (i + 1)
    in
    loop 0

let matches_at_bytes data offset pattern mask =
  let plen = Bytes.length pattern in
  if Bytes.length data < offset + plen then false
  else
    let rec loop i =
      if i >= plen then true
      else
        let m = Bytes.get_uint8 mask i in
        if
          Bytes.get_uint8 data (offset + i) land m
          <> Bytes.get_uint8 pattern i land m
        then false
        else loop (i + 1)
    in
    loop 0

let get_be_u32 data offset =
  (Bytes.get_uint8 data offset lsl 24)
  lor (Bytes.get_uint8 data (offset + 1) lsl 16)
  lor (Bytes.get_uint8 data (offset + 2) lsl 8)
  lor Bytes.get_uint8 data (offset + 3)

let simple_signatures =
  [ (* images *)
    (8, "\x89PNG\r\n\x1a\n", "image/png")
  ; (3, "\xff\xd8\xff", "image/jpeg")
  ; (6, "GIF87a", "image/gif")
  ; (6, "GIF89a", "image/gif")
  ; (2, "BM", "image/bmp")
  ; (4, "\x00\x00\x01\x00", "image/x-icon")
  ; (4, "\x00\x00\x02\x00", "image/x-icon")
  ; (* audio *)
    (3, "ID3", "audio/mpeg")
  ; (5, "OggS\x00", "audio/ogg")
  ; (8, "MThd\x00\x00\x00\x06", "audio/midi")
  ; (* other *)
    (5, "%PDF-", "application/pdf")
  ; (4, "PK\x03\x04", "application/zip")
  ; (3, "\x1f\x8b\x08", "application/gzip") ]

let check_simple data =
  List.find_map
    (fun (min_len, pattern, mime) ->
      if Bytes.length data >= min_len && matches_at data 0 pattern then
        Some mime
      else None )
    simple_signatures

(* RIFF container: bytes 0-3 = "RIFF", 4-7 = size (ignored), 8-11 = sub-format *)
let check_riff data =
  if Bytes.length data < 12 || not (matches_at data 0 "RIFF") then None
  else if matches_at data 8 "WAVE" then Some "audio/wave"
  else if matches_at data 8 "AVI " then Some "video/avi"
  else if Bytes.length data >= 14 && matches_at data 8 "WEBPVP" then
    Some "image/webp"
  else None

(* MP4: look for ftyp box and scan compatible brands for "mp4" *)
let check_mp4 data =
  if Bytes.length data < 12 then None
  else
    let box_size = get_be_u32 data 0 in
    if not (matches_at data 4 "ftyp") then None
    else if box_size < 8 || box_size land 3 <> 0 then None
    else
      let limit = min box_size (Bytes.length data) in
      (* major brand at offset 8, then compatible brands every 4 bytes from 16 *)
      let rec scan off =
        if off + 3 > limit then None
        else if matches_at data off "mp4" then Some "video/mp4"
        else scan (off + 4)
      in
      (* check major brand (offset 8) first, then compatible brands from 16 *)
      if limit >= 11 && matches_at data 8 "mp4" then Some "video/mp4"
      else scan 16

(* WebM: EBML header 1A 45 DF A3, then scan for DocType element 42 82
   containing "webm" *)
let check_webm data =
  if Bytes.length data < 4 then None
  else if
    Bytes.get_uint8 data 0 <> 0x1a
    || Bytes.get_uint8 data 1 <> 0x45
    || Bytes.get_uint8 data 2 <> 0xdf
    || Bytes.get_uint8 data 3 <> 0xa3
  then None
  else
    let limit = min 38 (Bytes.length data - 6) in
    let rec scan i =
      if i > limit then None
      else if
        Bytes.get_uint8 data i = 0x42 && Bytes.get_uint8 data (i + 1) = 0x82
      then
        (* next byte is VINT size; for "webm" it will be 0x84 (4) or 0x04 *)
        let size_byte = Bytes.get_uint8 data (i + 2) in
        let doc_len =
          if size_byte land 0x80 <> 0 then size_byte land 0x7f
          else if size_byte land 0x40 <> 0 then size_byte land 0x3f
          else size_byte
        in
        let doc_off = i + 3 in
        if doc_off + doc_len <= Bytes.length data && doc_len >= 4 then
          if matches_at data doc_off "webm" then Some "video/webm" else None
        else None
      else scan (i + 1)
    in
    scan 4

(* MP3 without ID3: frame sync 0xFF 0xE0 mask *)
let check_mp3_frame data =
  if Bytes.length data < 4 then None
  else if
    Bytes.get_uint8 data 0 <> 0xff || Bytes.get_uint8 data 1 land 0xe0 <> 0xe0
  then None
  else
    let b1 = Bytes.get_uint8 data 1 in
    let b2 = Bytes.get_uint8 data 2 in
    let layer = (b1 lsr 1) land 0x03 in
    let bitrate_idx = (b2 lsr 4) land 0x0f in
    let sample_idx = (b2 lsr 2) land 0x03 in
    if layer = 0 || bitrate_idx = 0 || bitrate_idx = 15 || sample_idx = 3 then
      None
    else Some "audio/mpeg"

let sniff data =
  match check_simple data with
  | Some mime ->
      mime
  | None -> (
    match check_riff data with
    | Some mime ->
        mime
    | None -> (
      match check_mp4 data with
      | Some mime ->
          mime
      | None -> (
        match check_webm data with
        | Some mime ->
            mime
        | None -> (
          match check_mp3_frame data with
          | Some mime ->
              mime
          | None ->
              "application/octet-stream" ) ) ) )
