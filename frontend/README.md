# frontend

is the web interface for pegasus, containing the admin dashboard and account management pages.

Built with [MLX](https://github.com/ocaml-mlx/mlx) (a JSX-like OCaml dialect), [server-reason-react](https://github.com/ml-in-barcelona/server-reason-react) (React SSR in OCaml) and [melange](https://melange.re) (OCaml to JavaScript compiler).

## pages

### admin

- **Login** (`/admin`) - Admin authentication
- **Users** (`/admin/users`) - View and manage PDS users
- **Invites** (`/admin/invites`) - Create and manage invite codes
- **Blobs** (`/admin/blobs`) - Monitor blob storage usage

### account

- **Account page** (`/account`) - User profile and email settings
- **Identity** (`/account/identity`) - Handle and DID management
- **Permissions** (`/account/permissions`) - OAuth app permissions and sessions
- **Login** (`/login`) - User authentication
- **Signup** (`/signup`) - New account creation

### oauth

- **Authorize** (`/oauth/authorize`) - OAuth authorization flow

## development

The frontend is built as part of the main pegasus project. When developing:

```bash
# Build the frontend
dune build

# The compiled JavaScript will be in _build/default/public/
```

### formatting

The frontend uses MLX syntax, which can't be formatted using ocamlformat:

```bash
# Install formatter
opam install ocamlformat-mlx

# Format MLX files
ocamlformat-mlx -i frontend/src/**/*.mlx
```

Note: You may see errors formatting files containing `[%browser_only]`. This is a known issue pending the next MLX release.
