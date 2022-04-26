#lang at-exp racket/base

(require db/base
         racket/bool
         racket/function
         scribble/srcdoc
         (for-syntax racket/syntax
                     syntax/parse
                     racket/base))

(provide
  db-mocking-data
  (form-doc
    (db-funcs-init prefix #:connection-func conn-func
                         [#:exn-fail-thunk exn-fail-thunk])
    #:contracts ([prefix any/c]
                 [connection-func (-> connection-pool? connection?)])
    @{
    })
)

(define db-mocking-data (make-parameter #f))

(define-for-syntax (create-identifier stx prefix name)
  (datum->syntax stx (string->symbol (format "~a-~a" (syntax->datum prefix) name))))

(define-syntax (db-funcs-init stx)
  (syntax-parse stx
    [(_ prefix (~seq #:connection-func conn-func)
        (~seq
          (~or
            (~optional (~seq #:exn-fail-thunk exn-fail-thunk)
                       #:defaults ([exn-fail-thunk #'(λ (e) (raise e))])))))
     #:with rest-connection (quote-syntax ...)
     #:with rest-transaction (quote-syntax ...)
     (with-syntax* ([query-funcs '(query-rows query-row query-list query query-exec query-maybe-row query-value query-maybe-value in-query)]
                    [query-func-names (map (λ (name) (create-identifier stx #'prefix name))
                                           (syntax->datum #'query-funcs))]
                    [connection-param (create-identifier stx #'prefix "connection")]
                    [connection-pool-param (create-identifier stx #'prefix "connection-pool")]
                    [prepare-name (create-identifier stx #'prefix "prepare")]
                    [disconnect-func (create-identifier stx #'prefix "disconnect!")]
                    [with-connection-name (datum->syntax stx (string->symbol (format "with-~a-connection" (syntax->datum #'prefix))))]
                    [with-transaction-name (datum->syntax stx (string->symbol (format "with-~a-transaction" (syntax->datum #'prefix))))]
                    [prefix-str (format "~a" (syntax->datum #'prefix))]
                    [get-func-sym #'(λ (func-name) (string->symbol (format "~a-~a" prefix-str func-name)))]
                    [query-func #'(λ (func-name-lst connection-param)
                                     (λ (stmt #:connection [user-conn #f] . args)
                                        (if (db-mocking-data)
                                          (let* ([data (hash-ref (db-mocking-data) (get-func-sym (cdr func-name-lst)))]
                                                 [ret (car data)])
                                            (when (immutable? (db-mocking-data)) (db-mocking-data (hash-copy (db-mocking-data))))
                                            (hash-set! (db-mocking-data) (get-func-sym (cdr func-name-lst)) (remove ret data))
                                            ret)
                                          (with-connection-name #:connection user-conn
                                            (apply (car func-name-lst) (append (list (connection-param) stmt)
                                                                               args))))))])
                   #'(begin
                       (define-namespace-anchor _a)
                       (define _ns (namespace-anchor->namespace _a))
                       (define connection-param (make-parameter #f))
                       (define connection-pool-param (make-parameter #f))
                       (define-values query-func-names (apply values (map (λ (func-name-lst) (query-func func-name-lst connection-param))
                                                                          (map (λ (x) (cons (eval x _ns) x)) (syntax->datum #'query-funcs)))))
                       (define (disconnect-func)
                         (when (connection? (connection-param))
                           (disconnect (connection-param))))
                       (define-syntax (with-connection-name stx)
                         (syntax-parse stx
                           ((_ (~optional (~seq #:connection user-conn) #:defaults ([user-conn #'#f])) body rest-connection)
                            (syntax/loc stx
                                        (let* ([owned (and (false? user-conn) (false? (connection-param)))]
                                               [the-conn (cond
                                                           [user-conn user-conn]
                                                           [owned
                                                             (conn-func (connection-pool-param))]
                                                           [else (connection-param)])])
                                          (parameterize ([connection-param the-conn])
                                            (with-handlers ([exn:fail? exn-fail-thunk])
                                                           (let ([ret ((thunk body rest-connection))])
                                                             (when owned
                                                               (disconnect-func))
                                                             ret))))))))
                       (define-syntax-rule (with-transaction-name thunk rest-transaction)
                         (with-connection-name
                           (call-with-transaction
                             (connection-param)
                             (λ () thunk rest-transaction))))
                       (define (prepare-name stmt)
                         (prepare (connection-param) stmt))))]))
