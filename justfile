async := "-collection:async=./async/async"

build:
    @-odin build . {{async}} -o:aggressive -show-timings

run:
    @-odin run . {{async}}
