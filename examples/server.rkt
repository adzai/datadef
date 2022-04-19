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


(define-conversion 'new? (λ (any) (~a any)))

(define-datadef users
  `((id _ (0 -1) number?) (name username (adam "user") new?) (value _ (1 0) boolean?))
  #:ret-type hash
  #:from "test_table"
  #:provide)

(define-datadef single-user
  '(name)
  #:ret-type hash
  #:from "test_table"
  #:single-ret-val/f
  #:where "id=$1")

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
           net/url-structs
           syntax/location)
  (define (exported? id)
    (define-values (_ exports) (module->exports (last (string-split (quote-source-file) "/" ))))
      (for/or ([export exports])
        (for/or ([e export])
          (and (list? e) (eq? (car e) id)))))
  (test-case
    "Testing that datadef:users is exported"
    (check-true (exported? 'datadef:users)))
  (test-case
    "Testing that datadef:single-user is not exported"
    (check-false (exported? 'datadef:single-user)))
  (test-case
    "Testing users servlet"
    (parameterize ([db-mocking-data (make-immutable-hash `((,datadef:users . ((0 1) 1))
                                          #;(dtb-query-rows . ,(make-list 2 (list (vector 1 "adam" 7))))))])
      (define req (make-request #"GET" (string->url "http://racket-lang.org")
                                '() (delay #t) #f "" 1111 ""))
      (check-equal? (response-code (get-users req)) 200))))
