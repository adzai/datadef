#lang at-exp racket/base

(require db/base
         racket/bool
         racket/function
         racket/list
         racket/match
         scribble/srcdoc
         (for-doc scribble/example
                  scribble/manual)
         "lib/test-utils.rkt"
         "lib/utils.rkt"
         "lib/pooling.rkt"
         (for-syntax racket/syntax
                     syntax/parse
                     racket/base))

(provide
  (form-doc
    (with-mock-data (~or mock-data-definition #:datadef) body ...)
    @{
    Defines a block where all database data will be mocked.
    First value to be provided is a list of lists defining the mock data.
    First value of this list is the identifier that will be mocked,
    such as a datadef or a query function generated via the dtb module.
    The second value for datadef entries is expected to be the position
    of mock data in the datadef definition, for dtb functions it is expected to be a
    list of mock data.
    The dtb functions can optionally receive a third value, which can be a list of
    indices that will be used to retrieve data from the mock data defined in the
    second value.

    An optional argument @racket[#:datadef] can be provided, which consumes mock data
    from the datadef definition one by one.

    @examples[#:label "Example tests:"
          (require rackunit
                   db
                   datadef)
          (db-funcs-init dtb)
          (test-case
              "Testing list of hash and empty list return type"
              (define-datadef test
                              '((column1 _ (val1 val2)) (column2 _ (val3 val4)))
                              #:ret-type hash
                              #:from "table")
              (with-mock-data ((datadef:test ((0 1) #f))
                               (dtb-query-rows ((#(val1 val2 val3)) ()) (0 1)))
                (check-equal? (datadef:test->result)
                              `(,#hash([column1 . val1]
                                       [column2 . val3])
                                ,#hash([column1 . val2]
                                       [column2 . val4])))
                (check-equal? (datadef:test->result)
                              '())
                (check-equal? (dtb-query-rows "SELECT * FROM table")
                              '(#(val1 val2 val3)))
                (check-equal? (dtb-query-rows "SELECT * FROM another_table")
                              '()))
              (with-mock-data #:datadef
                (check-equal? (datadef:test->result)
                              `(,#hash([column1 . val1]
                                       [column2 . val3])))))
          ]})
  db-mocking-data
  (form-doc
    (db-funcs-init prefix
                         [#:exn-fail-thunk exn-fail-thunk])
    #:contracts ([prefix any/c]
                 [connection-func (-> db-connection-pool? connection?)])
    @{
    Creates wrappers around @hyperlink["https://docs.racket-lang.org/db/query-api.html" "db"]
    query functions with the provided prefix. For
    example, if the user provides a prefix @racket[dtb], functions such as
    @racket[query-rows] will be wrapped and called @racket[dtb-query-rows].
    The wrapped functions provide some benefits, such as handling the database
    connection and deciding if data should be mocked.

    })
  create-db-connection-pool
  get-connection
  return-connection
  close-connection
  clear-pool
)

; SPEC
#; (with-mock-data
  '((datadef:test 0 1 2 3)
    (dtb-query-rows (("test" 1 2 3) (4 5 6 5)) 0 0 0 0))
  (check-equal?))
(define-syntax (with-mock-data stx)
  (syntax-parse stx
    [(_ (~or (~seq (mock-data ...) (~and #:datadef datadef-kw) mock+dd)
             (~or (mock-data ...) (~and #:datadef datadef-kw) mock/dd))
                   body ...)
     (with-syntax ([mock-data-list (cond
                                     [(attribute mock+dd)
                                      #'(syntax->datum #'(mock-data ...))]
                                     [(attribute datadef-kw) #'#f]
                                     [else
                                     #'(syntax->datum #'(mock-data ...))])]
                   [datadef? (if (attribute datadef-kw) #t #f)])
       #'(begin
           (parameterize ([db-mocking-data (make-hash)])
             (when datadef? (hash-set! (db-mocking-data) 'datadef #t))
             (when mock-data-list (set-mock-data! mock-data-list))
             body ...)))]))

(define-for-syntax (create-identifier stx prefix name)
  (datum->syntax stx (string->symbol (format "~a-~a" (syntax->datum prefix) name))))

(define-syntax (db-funcs-init stx)
  (syntax-parse stx
    [(_ prefix
        (~seq
          (~or
            (~optional (~seq #:exn-fail-thunk exn-fail-thunk)
                       #:defaults ([exn-fail-thunk #'(λ (e) (raise e))])))))
     #:with rest-connection (quote-syntax ...)
     #:with rest-transaction (quote-syntax ...)
     (with-syntax* ([query-funcs '(query-rows query-row query-list query query-exec query-maybe-row query-value query-maybe-value)]
                    [query-func-names (map (λ (name) (create-identifier stx #'prefix name))
                                           (syntax->datum #'query-funcs))]
                    [connection-param (create-identifier stx #'prefix "connection")]
                    [connection-pool-param (create-identifier stx #'prefix "connection-pool")]
                    [prepare-name (create-identifier stx #'prefix "prepare")]
                    [in-query-name (create-identifier stx #'prefix "in-query")]
                    [return-func (create-identifier stx #'prefix "conn-return")]
                    [close-func (create-identifier stx #'prefix "close-connection!")]
                    [start-func (create-identifier stx #'prefix "start!")]
                    [with-connection-name (datum->syntax stx (string->symbol (format "with-~a-connection" (syntax->datum #'prefix))))]
                    [with-transaction-name (datum->syntax stx (string->symbol (format "with-~a-transaction" (syntax->datum #'prefix))))]
                    [prefix-str (format "~a" (syntax->datum #'prefix))]
                    [get-func-sym #'(λ (func-name) (string->symbol (format "~a-~a" prefix-str func-name)))]
                    [query-func #'(λ (func-name-lst connection-param)
                                     (λ (stmt #:connection [user-conn #f] . args)
                                        (if (db-mocking-data)
                                          (let* ([mock (hash-ref (db-mocking-data) (get-func-sym (cdr func-name-lst)))]
                                                 [positions (db-mock-positions mock)]
                                                 [pos (cond
                                                        [(list? positions)
                                                         (car positions)]
                                                        [(void? positions) 0]
                                                        [(false? positions) #f]
                                                        [else (list positions)])]
                                                 [data (db-mock-data mock)])
                                            (unless (empty? positions)
                                              (hash-set! (db-mocking-data)
                                                         (get-func-sym (cdr func-name-lst))
                                                         (db-mock data (if (and (list? positions)
                                                                                (not (empty? positions)))
                                                                         (remove pos positions)
                                                                         positions))))
                                            (for/list ([d (list-ref data pos)])
                                              (for/vector ([v d])
                                                (with-handlers ([exn:fail? (λ (e) v)])(eval v _ns)))))
                                          (with-connection-name #:connection user-conn
                                                                (apply (car func-name-lst) (append (list (db-connection-raw-connection (connection-param)) stmt)
                                                                                                   args))))))])
                   #'(begin
                       (define-namespace-anchor _a)
                       (define _ns (namespace-anchor->namespace _a))
                       (define connection-param (make-parameter #f))
                       (define connection-pool-param (make-parameter #f))
                       (define (start-func connection-thunk #:max-connections [max-connections +inf.0]
                                           #:pool-limit-retries [pool-retries 0]
                                           #:conn-error-retries [conn-retries 0]
                                           #:sleep-on-pool-limit-retry [sleep-on-pool-limit-retry-seconds 1]
                                           #:sleep-on-conn-error-retry [sleep-on-conn-error-retry-seconds 1])
                         (connection-pool-param (create-db-connection-pool
                                                  (procedure-rename connection-thunk (string->symbol (format "~a-connection-thunk" prefix-str)))
                                                  #:prefix prefix-str
                                                  #:max-connections max-connections
                                                  #:pool-limit-retries pool-retries
                                                  #:conn-error-retries conn-retries
                                                  #:sleep-on-pool-limit-retry sleep-on-pool-limit-retry-seconds
                                                  #:sleep-on-conn-error-retry sleep-on-pool-limit-retry-seconds)))
                       (define-values query-func-names (apply values (map (λ (func-name-lst) (query-func func-name-lst connection-param))
                                                                          (map (λ (x) (cons (eval x _ns) x)) (syntax->datum #'query-funcs)))))
                       (define (return-func)
                         (when (db-connection? (connection-param))
                           (return-connection (connection-param) (connection-pool-param))
                           (connection-param #f)))
                       (define (close-func)
                         (when (db-connection? (connection-param))
                           (close-connection (connection-param) (connection-pool-param))
                           (connection-param #f)))
                       (define-syntax (with-connection-name stx)
                         (syntax-parse stx
                           ((_ (~optional (~seq #:connection user-conn) #:defaults ([user-conn #'#f])) body rest-connection)
                            (syntax/loc stx
                                        (let* ([owned (and (false? user-conn) (false? (connection-param)) (not (db-mocking-data)))]
                                               [the-conn (cond
                                                           [(db-mocking-data) #f]
                                                           [user-conn user-conn]
                                                           [owned
                                                             (get-connection (connection-pool-param))]
                                                           [else (connection-param)])])
                                          (parameterize ([connection-param the-conn])
                                            (with-handlers ([exn:fail? exn-fail-thunk])
                                                           (let ([ret ((thunk body rest-connection))])
                                                             (when owned
                                                               (return-func))
                                                             ret))))))))
                       (define-syntax-rule (with-transaction-name thunk rest-transaction)
                         (with-connection-name
                           (call-with-transaction
                             (db-connection-raw-connection (connection-param))
                             (λ () thunk rest-transaction))))
                       (define (in-query-name #:fetch (fetch #f) stmt . args)
                         (with-connection-name
                           (if fetch
                             (apply in-query (db-connection-raw-connection (connection-param)) stmt (flatten args) #:fetch fetch)
                             (apply in-query (db-connection-raw-connection (connection-param)) stmt (flatten args)))))
                       (define (prepare-name stmt)
                         (prepare (db-connection-raw-connection (connection-param)) stmt))))]))


(module+ test
  (require rackunit
           db)
  (db-funcs-init dtb #:exn-fail-thunk (λ (e) (close-connection (dtb-connection) (dtb-connection-pool))))
  (db-funcs-init dtb2 #:exn-fail-thunk (λ (e) (close-connection (dtb2-connection) (dtb2-connection-pool))))
  (db-funcs-init test #:exn-fail-thunk (λ (e) (close-connection (test-connection) (test-connection-pool))))
  (test-case "Pool limit reached"
    (dtb-start! (thunk (sqlite3-connect #:database "test-utils/test.db")) #:max-connections 1)
    (define t (thread (thunk (with-dtb-connection (sleep 0.2) (dtb-query-value "SELECT value FROM test_table")))))
    (sleep 0.1)
    (check-exn exn:pool-limit-reached?
               (thunk (dtb-query-rows "SELECT * FROM test_table")))
    (thread-wait t))
  (test-case "Pool limit not reached"
    (dtb-start! (thunk (sqlite3-connect #:database "test-utils/test.db")) #:max-connections 2)
    (define t (thread (thunk (with-dtb-connection (sleep 0.2) (dtb-query-value "SELECT value FROM test_table")))))
    (sleep 0.1)
    (check-not-exn
               (thunk (dtb-query-rows "SELECT * FROM test_table")))
    (thread-wait t))
  (test-case "Retry logic"
    (dtb-start! (thunk (sqlite3-connect #:database "test-utils/test.db")) #:max-connections 1
                #:pool-limit-retries 10
                #:sleep-on-pool-limit-retry 0.5
                #:conn-error-retries 5
                #:sleep-on-conn-error-retry 2)
    (define t (thread (thunk (with-dtb-connection (sleep 0.2) (dtb-query-value "SELECT value FROM test_table")))))
    (sleep 0.1)
    (check-equal?
      (dtb-query-rows "SELECT * FROM test_table") '(#(1)))
    (thread-wait t))
  (test-case "Only 1 connection was allocated and reused"
    (dtb-start! (thunk (sqlite3-connect #:database "test-utils/test.db"))
                #:pool-limit-retries 5
                #:sleep-on-pool-limit-retry 1)
    (dtb-query-rows "SELECT * FROM test_table")
    (dtb-query-rows "SELECT * FROM test_table")
    (dtb-query-rows "SELECT * FROM test_table")
    (dtb-query-rows "SELECT * FROM test_table")
    (check-equal? (db-connection-pool-connections-acquired (dtb-connection-pool)) 1))
  (test-case "2 connections were allocated"
    (dtb-start! (thunk (sqlite3-connect #:database "test-utils/test.db")) #:max-connections +inf.0)
    (define t (thread (thunk (with-dtb-connection (sleep 0.2) (dtb-query-value "SELECT value FROM test_table")))))
    (sleep 0.1)
    (dtb-query-rows "SELECT * FROM test_table")
    (thread-wait t)
    (check-equal? (db-connection-pool-connections-acquired (dtb-connection-pool)) 2))
  (test-case "Close connection via fail thunk"
    (dtb-start! (thunk (sqlite3-connect #:database "test-utils/test.db")) #:max-connections +inf.0)
    (with-dtb-connection (error "TEST"))
    (check-equal? (db-connection-pool-allocated-connections-number (dtb-connection-pool)) 0)
    (check-equal? (length (db-connection-pool-connections (dtb-connection-pool))) 0))
  (test-case "2 pools"
    (dtb-start! (thunk (sqlite3-connect #:database "test-utils/test.db")) #:max-connections +inf.0 #:pool-limit-retries 10)
    (dtb2-start! (thunk (sqlite3-connect #:database "test-utils/test.db")) #:max-connections +inf.0 #:pool-limit-retries 10)
    (dtb-query-rows "SELECT * FROM test_table")
    (dtb2-query-rows "SELECT * FROM test_table")
    (check-equal? (db-connection-pool-allocated-connections-number (dtb-connection-pool)) 1)
    (check-equal? (db-connection-pool-allocated-connections-number (dtb2-connection-pool)) 1))
  (test-case "All connections were cleared"
      (dtb-start! (thunk (sqlite3-connect #:database "test-utils/test.db")) #:max-connections +inf.0)
      (define t (thread (thunk (with-dtb-connection (sleep 0.2) (dtb-query-value "SELECT value FROM test_table")))))
      (sleep 0.1)
      (dtb-query-rows "SELECT * FROM test_table")
      (thread-wait t)
      (check-equal? (db-connection-pool-allocated-connections-number (dtb-connection-pool)) 2)
      (clear-pool (dtb-connection-pool))
      (check-equal? (db-connection-pool-allocated-connections-number (dtb-connection-pool)) 0))
)
