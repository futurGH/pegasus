# pegasus

is the core library implementing the PDS functionality.

## architecture

```
pegasus/lib/
├── api/              # XRPC API endpoints
│   ├── account_/     # Account management UI
│   ├── admin/        # com.atproto.admin.* XRPC endpoints
│   ├── admin_/       # Admin UI
│   ├── identity/     # com.atproto.identity.* XRPC endpoints
│   ├── oauth_/       # OAuth flows
│   ├── repo/         # com.atproto.repo.* XRPC endpoints
│   ├── server/       # com.atproto.server.* XRPC endpoints
│   └── sync/         # com.atproto.sync.* XRPC endpoints
├── oauth/            # OAuth implementation
├── lexicons/         # Generated atproto types
├── migrations/       # Database schema migrations
├── s3/               # S3 blob storage backend
├── auth.ml           # Authentication logic
├── blob_store.ml     # Blob storage interface
├── data_store.ml     # Database interface
├── env.ml            # Environment configuration
├── id_resolver.ml    # Identity resolution
├── jwt.ml            # JWT token handling
├── plc.ml            # PLC directory client
├── rate_limiter.ml   # Rate limiting
├── repository.ml     # Repository operations
├── sequencer.ml      # Event sequencing
├── session.ml        # Session management
└── user_store.ml     # User data access
```

## storage

### database

Currently only SQLite is supported. Open to pull requests for other databases!

### blob storage

Supports two backends:

- **Local filesystem** - Stores blobs in `{PDS_DATA_DIR}/blobs/{did}/{cid}`
- **S3-compatible** - Stores blobs in S3 bucket

Configurable via environment variables (see main README).

## email notifications

Optional email support for:

- Email address verification
- Password reset
- Identity update confirmation
- Account deletion confirmation

Falls back to stdout logging if SMTP not configured.

## contributing

To add new endpoints:

1. Add handler in `./lib/api/`
2. Register route in [`bin/main.ml`](bin/main.ml)
3. If frontend, also add to [`frontend/client/Router.mlx`](frontend/client/Router.mlx)

## testing

Tests are in `pegasus/test/`:

```bash
dune test
```

## environment

Configuration is loaded from environment variables. See main README for configuration options.
