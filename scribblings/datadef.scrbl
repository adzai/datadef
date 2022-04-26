#lang scribble/manual

@require[scribble/extract
	scribble/example
	racket/sandbox
	scribble/core
	scribble/xref
	scribble/html-properties
	racket/runtime-path
	racket/string]

@title[#:version "0.0.1"]{Datadef}
Database data retrieval and database data mocking.

@(table-of-contents)

@section{Introduction}
@defmodule[datadef]

Provides exports from both @secref["dd"] and @secref["dtb"], but each
module can be used separately.

@section[#:tag "dd"]{Datadef}
@defmodule[datadef/dd]
@include-extracted["../dd.rkt"]

@section[#:tag "dtb"]{Dtb}
@defmodule[datadef/dtb]
@include-extracted["../dtb.rkt"]

@section{Utils}
@defmodule[datadef/lib/utils]
@include-extracted["../lib/utils.rkt"]
