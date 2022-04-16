#lang at-exp racket/base

(require db
         racket/bool
         racket/function
         racket/provide
         (for-syntax racket/syntax
                     syntax/parse
                     racket/base))

(provide dtb-funcs-init
         db-mocking?
         db-mocking-data)

(define db-mocking? (make-parameter #f))
(define db-mocking-data (make-parameter #f))

(define-for-syntax (create-identifier stx prefix name)
  (datum->syntax stx (string->symbol (format "~a-~a" (syntax->datum prefix) name))))

(define-syntax (dtb-funcs-init stx)
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
                                    (λ (stmt . args)
                                       (if (db-mocking?)
                                         (let* ([data (hash-ref (db-mocking-data) (get-func-sym (cdr func-name-lst)))]
                                                [ret (car data)])
                                           (when (immutable? (db-mocking-data)) (db-mocking-data (hash-copy (db-mocking-data))))
                                           (hash-set! (db-mocking-data) (get-func-sym (cdr func-name-lst)) (remove ret data))
                                           ret)
                                       (with-connection-name
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
             (syntax-case stx ()
               ((_ body rest-connection)
                (syntax/loc stx
                            (let* ([owned (false? (connection-param))]
                                   [the-conn (if owned
                                               (conn-func (connection-pool-param))
                                               (connection-param))])
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
                  (λ ()
                     thunk rest-transaction
                     ))))
            (define (prepare-name stmt)
              (prepare (connection-param) stmt))
           ))]))
