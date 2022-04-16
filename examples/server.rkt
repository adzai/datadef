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
  (define dat1 (datadef:users->result #:json #t))
  (displayln (format "DATA 1: ~v" dat1))
  (define dat2 (datadef:users->result #:json #t))
  (displayln (format "DATA 2: ~v" dat2))
  (response/jsexpr (make-immutable-hash `([data . ,dat1]))))

(define/provide-datadef users
  '((id _ (0 -1)) (name username ("adam" "user")) (value _ (6 777)))
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

(module+ main
  (start-server!))

(module+ test
  (require rackunit
           net/url-structs)
  (test-case
    "Testing users servlet"
    (parameterize ([db-mocking? #t]
                   [db-mocking-data (make-immutable-hash `(#;(,datadef:users . ((0 1) 1))
                                          (dtb-query-rows . ,(make-list 2 (list (vector 1 "adam" 7))))))])
      (define req (make-request #"GET" (string->url "http://racket-lang.org")
                                '() (delay #t) #f "" 1111 ""))
      (check-equal? (response-code (get-users req)) 200))))
