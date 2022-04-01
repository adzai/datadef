; Example of a user defined dtb module using dtb-module and datadef
#lang at-exp racket/base

(require db
         "../dtb-module.rkt"
         "../datadef.rkt"
         racket/bool
         racket/string
         racket/format
         racket/list
         racket/class)

(provide dtb-connect!)

(dtb-funcs-init dtb
                #:connection-func try-pool-connection
                #:exn-fail-thunk (λ (e)
                         (dtb-disconnect!)
                         (raise e)))

(datadef-db-rows-func dtb-query-rows)

(define (try-pool-connection pool
                        #:tries [tries 20]
                        #:sleep-length [sleep-length 0])
  (with-handlers
    ([exn:fail?
       (λ (e)
          (let* ([msg (exn-message e)]
                 [connection-failed?
                   (regexp-match
                     #rx"connection failed"
                     msg)]
                 [pool-limit-reached?
                   (regexp-match
                     #rx"pool limit reached"
                     msg)])
            (log-error msg)
            (if (> tries 0)
              (cond
                [connection-failed?
                  (log-debug "Connection failed, attempting reconnect")
                  (sleep sleep-length)
                  (try-pool-connection pool #:tries (- tries 4)
                                            #:sleep-length 5)]
                [pool-limit-reached?
                  (log-debug "Pool limit reached, attempting reconnect")
                  (sleep sleep-length)
                  (try-pool-connection pool #:tries (sub1 tries)
                                            #:sleep-length 2)]
                [else (raise e)])
              (raise e))))])
    (connection-pool-lease pool)))

(define (dtb-connect!)
  (dtb-connection-pool
    (connection-pool
      (λ () (sqlite3-connect #:database "examples/test.db")))))


(define (example-func)
    (dtb-query-rows "SELECT * FROM users"))

(module+ test
  (require rackunit)
  (test-case
    "test case"
    (parameterize ([dtb-mocking? #t]
                   [dtb-mocking-data  (make-hash
                                        `([dtb-query-rows (userinfo) (other-info)]))])
    (check-equal? (example-func) '(userinfo)))))
