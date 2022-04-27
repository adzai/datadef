#lang scribble/manual
@require[scribble/extract]
@title[#:version "0.1"]{Example documentation}

@(table-of-contents)

@section{Datadef}
@defmodule[datadef]
@declare-exporting["../dd.rkt"]
@include-extracted["../dd.rkt"]

@section{Datadef lib}
@defmodule[datadef/lib/utils]
@declare-exporting["../lib/utils.rkt"]
@include-extracted["../lib/utils.rkt"]

@section{Auto generated documentation example}
@defmodule["server.rkt"]
@declare-exporting["../examples/server.rkt"]
@include-extracted["../examples/server.rkt"]
