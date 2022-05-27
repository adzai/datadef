#lang at-exp racket

(require db
         web-server/http
         web-server/servlet
         web-server/servlet-env
         web-server/dispatch
         "../main.rkt"
         "dtb.rkt")

(define dispatcher
  (dispatch-case
     [("users") #:method "get" get-users]))

(define (get-users req)
  (define dat1 (datadef:users->result #:json #t))
  (define dat2 (datadef:users->result #:json #t #:mutable #t))
  (response/jsexpr (make-immutable-hash `([data . ,dat1]))))


(define-conversion 'new? (λ (any) (~a any)))

(define-datadef users
  `((id _ (0 -1) number?) (name username (adam "user") new?) (value _ (1 0) number?))
  #:ret-type hash
  #:from "test_table"
  #:provide)

(define-datadef single-user
  '(name)
  #:ret-type hash
  #:from "test_table"
  #:single-ret-val
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
    #| (parameterize ([db-mocking-data (make-immutable-hash `(#;(datadef:users . ((0 1) 1)) |#
    #|                                       (dtb-query-rows . ,(make-list 2 (list (vector 1 "adam" 7))))))]) |#
      (with-mock-data ((dtb-query-rows ((#(1 "adam" 7)))))
      (define req (make-request #"GET" (string->url "http://racket-lang.org")
                                '() (delay #t) #f "" 1111 ""))
      (check-equal? (response-code (get-users req)) 200)))
  (test-case "db mock with #:datadef"
      (define-datadef test
                      '((column1 _ (val1)) (column2 _ (val2)))
                      #:ret-type hash
                      #:from "table")
      (with-mock-data ((dtb-query-rows ((#(1 2 3)))))
                      #:datadef
         (check-equal? (datadef:test->result)
                      `(,#hash([column1 . val1]
                               [column2 . val2])))
         (check-equal? (list (vector 1 2 3)) (dtb-query-rows "SELECT * FROM TEST")))
      (with-mock-data #:datadef
                    ((dtb-query-rows ((#(1 2 3)) (#(3 4 5) #(6 7 8))) (1)))
         (check-equal? (datadef:test->result)
                      `(,#hash([column1 . val1]
                               [column2 . val2])))
         (check-equal? (list (vector 3 4 5) (vector 6 7 8)) (dtb-query-rows "SELECT * FROM TEST"))))
)
