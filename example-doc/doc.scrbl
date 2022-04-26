#lang scribble/manual
@require[scribble/extract]
@title[#:version "0.1"]{Example documentation}

@(table-of-contents)

@section{Datadef}
@defmodule[datadef]
@declare-exporting["../main.rkt"]
@include-extracted["../main.rkt"]

@section{Auto generated documentation example}
@defmodule["server.rkt"]
@declare-exporting["../examples/server.rkt"]
@include-extracted["../examples/server.rkt"]
