FROM ocaml/opam:debian-12-ocaml-5.2 AS build

RUN sudo apt-get install -y cmake git libev-dev libffi-dev libgmp-dev libssl-dev libsqlite3-dev pkg-config

WORKDIR /home/opam

ADD . .
RUN opam install . --deps-only --with-test --unlock-base
RUN opam exec -- dune build


FROM ocaml/opam:debian-12-ocaml-5.2 AS run

RUN sudo apt-get install -y libev-dev

COPY --from=build /home/opam/_build/default/bin/main.exe /bin/pegasus

ENTRYPOINT ["/bin/pegasus"]
