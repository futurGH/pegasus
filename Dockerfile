FROM ocaml/opam:debian-12-ocaml-5.2 AS build

ARG NODE_VERSION=v24.11.1
ARG OPAM_VERSION=2.5.0
ARG DUNE_VERSION=3.20.2

ARG GIT_REV
ENV GIT_REV=$GIT_REV

USER root
RUN apt-get update && apt-get install -y cmake git libev-dev libffi-dev libgmp-dev libssl-dev libsqlite3-dev libpcre3-dev pkg-config

RUN bash -c "curl -fsSL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh | bash -s -- --version $OPAM_VERSION"
RUN bash -c "chown opam:opam /bin/opam"
USER opam

WORKDIR /home/opam/pegasus

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash

ENV NVM_DIR=/home/opam/.nvm
ENV CI=true

RUN bash -c "source $NVM_DIR/nvm.sh && nvm install $NODE_VERSION && nvm alias default $NODE_VERSION && nvm use default && corepack enable pnpm"

ADD . .

RUN bash -c "source $NVM_DIR/nvm.sh && pnpm install --frozen-lockfile"

ENV DUNE_CACHE="enabled"
RUN --mount=type=cache,target=/home/opam/.opam/download-cache,uid=1000,gid=1000 \
    --mount=type=cache,target=/home/opam/.cache/dune,uid=1000,gid=1000 \
    opam install dune.$DUNE_VERSION
RUN --mount=type=cache,target=/home/opam/.cache/dune,uid=1000,gid=1000 \
    opam exec dune pkg lock
RUN --mount=type=cache,target=/home/opam/.cache/dune,uid=1000,gid=1000 \
    bash -c "source $NVM_DIR/nvm.sh && opam exec dune build -- --release --stop-on-first-error"

FROM debian:12 AS run

RUN apt-get update && apt-get install -y ca-certificates cmake git libev-dev libffi-dev libgmp-dev libssl-dev libsqlite3-dev libpcre3-dev pkg-config netbase

RUN mkdir /data

COPY --from=build /home/opam/pegasus/_build/default/bin/main.exe /bin/pegasus
COPY --from=build /home/opam/pegasus/_build/default/bin/gen_keys.exe /bin/gen-keys

ENTRYPOINT ["/bin/pegasus"]

LABEL org.opencontainers.image.source="https://github.com/futurgh/pegasus"
LABEL org.opencontainers.image.description="pegasus, an atproto pds"
LABEL org.opencontainers.image.licenses=MPL-2.0
