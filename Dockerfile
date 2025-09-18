FROM --platform=linux/amd64 ocaml/opam:debian-12-ocaml-5.2 AS build

RUN sudo apt-get install -y cmake git libev-dev libffi-dev libgmp-dev libssl-dev libsqlite3-dev pkg-config

WORKDIR /home/opam

RUN curl -fsSL https://get.dune.build/install | sh

ENV PATH="/home/opam/.local/bin:${PATH}"
ENV DUNE_CACHE="enabled"

ADD . .
RUN dune pkg lock
RUN dune build

FROM --platform=linux/amd64 ocaml/opam:debian-12-ocaml-5.2 AS run

RUN sudo apt-get install -y libev-dev

COPY --from=build /home/opam/_build/default/bin/main.exe /bin/pegasus

ENTRYPOINT ["/bin/pegasus"]
