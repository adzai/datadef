#lang scribble/manual

@require[scribble/extract
	scribble/example
	racket/sandbox
	scribble/core
	scribble/xref
	scribble/html-properties
	racket/runtime-path
	racket/string]

@title[#:version "0.0.1"]{datadef}
@italic{A library for database data retrieval and database data mocking.}

@author["Adam Zaiter"]


@defmodule[datadef]

Provides exports from both @secref["dd"] and @secref["dtb"], but each
module can be used separately.

@section[#:tag "dd"]{datadef}
@defmodule[datadef/dd]
@include-extracted["../dd.rkt"]

@section[#:tag "dtb"]{dtb}
@defmodule[datadef/dtb]
@include-extracted["../dtb.rkt"]

@section{utils}
@defmodule[datadef/lib/utils]
@include-extracted["../lib/utils.rkt"]
