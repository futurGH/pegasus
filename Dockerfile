FROM ocaml/opam:alpine-3.22-ocaml-5.2 AS build

ARG NODE_VERSION=v24.11.1
ARG OPAM_VERSION=2.5.0
ARG DUNE_VERSION=3.20.2

ARG GIT_REV
ENV GIT_REV=$GIT_REV

USER root
RUN apk add --no-cache cmake git libev-dev libffi-dev gmp-dev openssl-dev sqlite-dev pcre-dev pkgconfig

RUN bash -c "curl -fsSL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh | bash -s -- --version $OPAM_VERSION"
RUN bash -c "chown opam:opam /usr/bin/opam"
USER opam

WORKDIR /home/opam/pegasus

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash

ENV NVM_DIR=/home/opam/.nvm
ENV CI=true

RUN bash -c "source $NVM_DIR/nvm.sh && nvm install $NODE_VERSION && nvm alias default $NODE_VERSION && nvm use default && corepack enable pnpm"

ADD . .

RUN bash -c "source $NVM_DIR/nvm.sh && pnpm install --frozen-lockfile"

ENV DUNE_CACHE="enabled"
RUN opam install dune.$DUNE_VERSION
RUN opam exec dune pkg lock
RUN bash -c "source $NVM_DIR/nvm.sh && opam exec dune build -- --release --stop-on-first-error"

FROM alpine:3.22 AS run

RUN apk add --no-cache ca-certificates cmake git libev-dev libffi-dev gmp-dev openssl-dev sqlite-dev pcre-dev pkgconfig

RUN mkdir /data

COPY --from=build /home/opam/pegasus/_build/default/bin/main.exe /usr/bin/pegasus
COPY --from=build /home/opam/pegasus/_build/default/bin/gen_keys.exe /usr/bin/gen-keys

ENTRYPOINT ["/usr/bin/pegasus"]

LABEL org.opencontainers.image.source=https://tangled.org/futur.blue/pegasus
LABEL org.opencontainers.image.description="pegasus, an atproto pds"
LABEL org.opencontainers.image.licenses=MPL-2.0
