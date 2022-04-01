#lang at-exp racket

(require db
         web-server/http
         web-server/servlet
         web-server/servlet-env
         web-server/dispatch
         "../datadef.rkt"
         "dtb.rkt")

(define dispatcher
  (dispatch-case
     [("users") #:method "get" get-users]))

(define (get-users req)
  (response/jsexpr (make-immutable-hash `([data . ,(datadef:users->result #:json #t)]))))

(define/provide-datadef users
  '(id (name username) value)
  #:ret-type hash
  #:from "test_table")

(define (start-server!)
  (displayln "Starting server on port 7777")
  (dtb-connect!)
  (serve/servlet
    dispatcher
    #:port 7777
    #:command-line? #t
    #:stateless? #f
    #:servlet-regexp #rx""))

(start-server!)
