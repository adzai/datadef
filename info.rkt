#lang info

(define collection "datadef")

(define deps '("db-lib"
               "web-server-lib"
               "base" "scribble-lib"))

(define build-deps '("db-doc"
                     "at-exp-lib"
                     "rackunit-lib"
                     "sandbox-lib"
                     "scribble-lib" "racket-doc"))

(define scribblings '(("scribblings/datadef.scrbl" (multi-page))))

(define pkg-desc "Database data retrieval and database mocking")

(define pkg-authors '(adzai))

(define version "0.0.3")

(define license
  '(Apache-2.0 OR MIT))
