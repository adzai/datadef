# Datadef
Racket library for database data retrieval and database mocking.

## Requirements

- [racket](https://download.racket-lang.org/)
- [scribble](https://docs.racket-lang.org/scribble/): `raco pkg i scribble`
- [db](https://docs.racket-lang.org/db/): `raco pkg i db`


## Examples

Example usages of datadef and the dtb module are in
the examples folder.

**Generate documentation**

`cd docs`

`scribble +m doc.scrbl`

`open doc.html`

**Run example tests**

`cd examples`

`raco test dtb.rkt`

**Run example server**

`cd examples`

`racket server.rkt`

Test with curl:

`curl localhost:7777/users`
