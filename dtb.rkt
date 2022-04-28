#lang at-exp racket/base

(require db/base
         racket/bool
         racket/function
         racket/list
         racket/match
         scribble/srcdoc
         "lib/utils.rkt"
         (for-syntax racket/syntax
                     syntax/parse
                     racket/base))

(provide
  with-mock-data
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

; SPEC
#; (with-mock-data
  '((datadef:test 0 1 2 3)
    (dtb-query-rows (("test" 1 2 3) (4 5 6 5)) 0 0 0 0))
  (check-equal?))

(define (group-mock-data data-list positions)
  (define positions-list (if (list? positions)
                           positions
                           (list positions)))
  (for/list ([pos positions-list])
    (for/list ([data data-list])
      (list-ref data pos))))

 (define (parse-datadef-part dd part)
   (define len (length part))
   (match len
     [0 (db-mock #f (void))]
     [1 (db-mock #f (car part))]
     [2 (db-mock (car part) (cadr part))]
     [_ (error (format "Wrong number of args, part: ~a" part))] ; TODO better err msg
))

(define (parse-db-part data)
  (cond
    [(and (list? data) (= (length data) 1))
     (define dat (if (list? (car (car data))) (car data) (map list (car data))))
     (db-mock dat (void))]
    [(and (list? data) (= (length data) 2))
     (define dat (if (list? (car (car data))) (car data) (map list (car data))))
      (db-mock dat (cadr data))]
    [else (error (format "Wrong number of args, data: ~a" data))])) ; TODO better err msg


(define (set-mock-data! mock-data-list)
  (for ([part mock-data-list])
    (define key (car part))
    (define mock (if (regexp-match? #rx"datadef:" (symbol->string key))
      (parse-datadef-part key (cdr part))
      (parse-db-part (cdr part))))
    (hash-set! (db-mocking-data) key mock)))

(define-syntax (with-mock-data stx)
  (syntax-parse stx
    [(_ (mock-data ...) body ...)
     (with-syntax ([mock-data-list #'(syntax->datum #'(mock-data ...))])
       #'(begin
           (parameterize ([db-mocking-data (make-hash)])
             (set-mock-data! mock-data-list)
             body ...)))]))

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
                                          (let* ([mock (hash-ref (db-mocking-data) (get-func-sym (cdr func-name-lst)))]
                                                 [positions (db-mock-positions mock)]
                                                 [pos (cond
                                                        [(list? positions)
                                                        (car positions)]
                                                        [(void? positions) '(0)]
                                                        [(false? positions) #f]
                                                        [else (list positions)])]
                                                [data (car (db-mock-data mock))]
                                                [rest-data (cdr (db-mock-data mock))])
                                            (unless (void? positions)
                                              (hash-set! (db-mocking-data) (syntax->datum #'datadef:name) (db-mock rest-data (if (and (list? positions)
                                                                                                                                           (not (empty? positions)))
                                                                                                                                    (remove pos positions)
                                                                                                                                    positions))))
                                            (get-mock-data data pos))
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
