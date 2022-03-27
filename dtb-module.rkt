#lang at-exp racket/base

(require db
         racket/bool
         racket/function
         racket/provide
         (for-syntax racket/syntax
                     syntax/parse
                     racket/base))

(provide dtb-funcs-init)

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
                    [server-param (create-identifier stx #'prefix "server")]
                    [port-param (create-identifier stx #'prefix "port")]
                    [username-param (create-identifier stx #'prefix "username")]
                    [password-param (create-identifier stx #'prefix "password")]
                    [database-param (create-identifier stx #'prefix "database")]
                    [pooling-param (create-identifier stx #'prefix "pooling")]
                    [connection-param (create-identifier stx #'prefix "connection")]
                    [connection-pool-param (create-identifier stx #'prefix "connection-pool")]
                    [prepare-name (create-identifier stx #'prefix "prepare")]
                    [disconnect-func (create-identifier stx #'prefix "disconnect!")]
                    [with-connection-name (datum->syntax stx (string->symbol (format "with-~a-connection" (syntax->datum #'prefix))))]
                    [with-transaction-name (datum->syntax stx (string->symbol (format "with-~a-transaction" (syntax->datum #'prefix))))]
                    [query-func #'(λ (func-name connection-param)
                                    (λ (stmt . args)
                                       (with-connection-name
                                         (apply func-name (append (list (connection-param) stmt)
                                                                    args)))))])
       #'(begin
           (define-namespace-anchor a)
           (define ns (namespace-anchor->namespace a))
           (provide
             (filtered-out
               (λ (name) (and (member (string->symbol name) (syntax->datum #'query-func-names)) name))
               (all-defined-out)))
           (define server-param (make-parameter #f))
           (define port-param (make-parameter #f))
           (define username-param (make-parameter #f))
           (define password-param (make-parameter #f))
           (define database-param (make-parameter #f))
           (define pooling-param (make-parameter 1))
           (define connection-param (make-parameter #f))
           (define connection-pool-param (make-parameter #f))
           (define-values query-func-names (apply values (map (λ (func-name) (query-func func-name connection-param))
                                                              (map (λ (x) (eval x ns)) (syntax->datum #'query-funcs)))))
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
