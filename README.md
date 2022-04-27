# Datadef
Racket library for database data retrieval and database mocking.

## Requirements

- [racket](https://download.racket-lang.org/)

## Install

`raco pkg install --auto --batch`

## Examples

Example usages of datadef and the dtb module are in
the examples folder.

**Generate documentation**

`cd example-doc`

`scribble +m doc.scrbl`

`open doc.html`

**Run tests**

`raco test **.rkt`

**Run example tests**

`cd examples`

`raco test dtb.rkt`

**Run example server**

`cd examples`

`racket server.rkt`

Test with curl:

`curl localhost:7777/users`
