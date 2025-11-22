FROM --platform=linux/amd64 ocaml/opam:debian-12-ocaml-5.2 AS build

ARG NODE_VERSION=v24.11.1
ARG DUNE_VERSION=3.20.2

RUN sudo apt-get install -y cmake git libev-dev libffi-dev libgmp-dev libssl-dev libsqlite3-dev pkg-config

WORKDIR /home/opam/pegasus

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash

ENV NVM_DIR=/home/opam/.nvm
ENV CI=true

RUN bash -c "source $NVM_DIR/nvm.sh && nvm install $NODE_VERSION && nvm alias default $NODE_VERSION && nvm use default && corepack enable pnpm"

RUN bash -c "curl -fsSL https://raw.githubusercontent.com/ocaml/opam/master/shell/install.sh | bash -s -- --dev"
RUN opam install dune.$DUNE_VERSION

ENV PATH="/home/opam/.local/bin:${PATH}"
ENV DUNE_CACHE="enabled"

ADD . .

RUN bash -c "source $NVM_DIR/nvm.sh && pnpm install --frozen-lockfile"

RUN opam exec dune pkg lock
RUN bash -c "source $NVM_DIR/nvm.sh && opam exec dune build -- --release --stop-on-first-error"

FROM --platform=linux/amd64 ocaml/opam:debian-12-ocaml-5.2 AS run

RUN sudo apt-get install -y libev-dev

COPY --from=build /home/opam/pegasus/_build/default/bin/main.exe /bin/pegasus

ENTRYPOINT ["/bin/pegasus"]
