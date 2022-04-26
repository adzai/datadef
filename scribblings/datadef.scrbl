#lang scribble/manual

@require[scribble/extract
	scribble/example
	racket/sandbox
	scribble/core
	scribble/html-properties
	racket/runtime-path
	racket/string]

@title[#:version "0.0.1"]{Datadef}

@(table-of-contents)
@section{Datadef}
@defmodule[datadef]
@include-extracted["../main.rkt"]


@section{Utils}
@defmodule[datadef/datadef-lib/utils]
@include-extracted["../lib/utils.rkt"]
