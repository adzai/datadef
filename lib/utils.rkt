#lang at-exp racket/base

(require scribble/srcdoc
         db/base
         (for-syntax racket/base)
         racket/bool
         "key-utils.rkt"
         racket/contract
         racket/match
         racket/list
         racket/string
         racket/format)

(provide
  get-datadef-mock-data
  define-conversion
  build-select-query
  format-query-string
  datadef->types
  (proc-doc/names
    datadef->keys
    (->* (list?)
         (#:doc boolean?)
         (listof symbol?))
    ((datadef)
     ((doc #f)))
    @{}
    )
  (proc-doc/names
    columns->keys
    (->* (list?)
         (#:doc boolean?)
         (listof symbol?))
    ((columns)
     ((doc #f)))
    @{}
    )
  (proc-doc/names
    datadef->columns
    (-> list? string?)
    (datadef)
    @{
    }
    )
  (proc-doc
    get-formatted-result
    (->i ((cols (listof symbol?)) (types (listof (or/c symbol? false?)))
                                  (iter-func (-> (listof symbol?) vector? boolean? (listof (or/c symbol? false?)) (or/c vector? list? hash?)))
                                  (rows (listof vector?))
                                  (json? boolean?)
          #:single-ret-val (single-ret-val boolean?)
          #:single-ret-val/f (single-ret-val/f boolean?)
          #:ret-type (ret-type procedure?))
         (#:custom-iter-func (custom-iter-func (or/c false? (-> (listof symbol?) vector? boolean? (or/c vector? list? hash?)))))
         (result (or/c list? hash? vector? false?)))
    (#f)
    @{
      Formats db rows and returns a result based on the datadef's return type.
    }
  )
  (proc-doc/names
     get-iter-func
     (-> procedure? symbol? (-> (listof symbol?) vector? boolean? (listof (or/c symbol? false?)) (or/c vector? list? hash?)))
     (ret-type case-type)
     @{
      Returns appropriate function for iterating over db rows based on the given
      datadef's return type.
     }
   )
)


(define/contract (register-conversion! pred proc)
  (-> symbol? (-> any/c any) void?)
  (hash-set! conversions pred proc))

(define-syntax (define-conversion stx)
  (syntax-case stx ()
    ((_ predicate procedure)
     (syntax/loc stx
       (register-conversion!
        predicate
        procedure)))))

(define conversions
  (make-hash
    `([string? . ,(λ (any) (if (string? any) any (~a any)))]
      [number? . ,(λ (any) (cond
                               [(number? any) any]
                               [else (define val (string->number (~a any)))
                                (if (number? val) val (error (format "Couldn't convert ~v to ~v" any number?)))]))]
      [char? . ,(λ (any)
                    (cond
                      [(char? any) any]
                      [else
                        (define lst (string->list (~a any)))
                        (if (= (length lst) 1)
                          (car lst)
                          (error (format "Couldn't convert ~v to char" any)))]))]
      [bytes? . ,(λ (any) (if (bytes? any) any (string->bytes/utf-8 (~a any))))]
      [symbol? . ,(λ (any) (if (symbol? any) any (string->symbol (~a any))))]
      )))

(define-conversion
  'boolean?
  (λ (any)
     (cond
       [(boolean? any) any]
       [else
         (define str (string-downcase (~a any)))
         (cond
           [(or (string=? str "0") (string=? str "1")) (string=? str "1")]
           [(string=? str "true") #t]
           [(string=? str "false") #f]
           [else (error (format "Couldn't convert ~v to bool" any))])])))

(define (get-datadef-key col)
  (cond
    [(list? col)
     (define key (cadr col))
     (if (eq? '_ key) (car col) key)]
    [else col]))

(define (get-key-list col)
  (define str-lst
    (string-split
      (symbol->string col)
      "."))
  (string->symbol
    (if (> (length str-lst) 1)
      (get-datadef-key str-lst)
      (car str-lst))))

(define (datadef->keys datadef #:doc [doc #f])
  (for/list ([dd datadef])
    (cond
      [(symbol? dd) (get-key-list dd)]
      [(list? dd)
       (get-key-list (if (eq? '_ (cadr dd))
                       (car dd)
                       (cadr dd)))]
      [else
        (error "Unknown element type in datadef")])))

(define (columns->keys columns #:doc [doc #f])
  (for/list ((col columns))
    (string->symbol
      (string-replace ; For future use in React.JS
        (~a (if (list? col) (get-datadef-key col) col))
        "."
        "_"))))

(define (datadef->columns datadef)
  (define strs
    (for/list ((dd datadef))
      (cond ((symbol? dd)
             (symbol->string dd))
            ((list? dd)
             (car dd))
            (else
             (error "Unknown element type in datadef")))))
  ;(writeln strs)
  (string-join
   strs
   ", "))

(define (datadef->types lst)
  (for/list ([elem lst])
    (if (and (list? elem) (= (length elem) 4))
      (cadddr elem)
      #f)))

(define (get-iter-func ret-type case-type)
  (cond
    [(eq? ret-type list)
     (λ (cols row json? types)
        (for/list ([val row]
                   [type? types])
          (define ret-val (if type? ((hash-ref conversions type?) val) val)) ; TODO cleanup conversions
          (if json? (ensure-json-value ret-val) ret-val)))]
    [(eq? ret-type vector)
     (λ (cols row json? types)
        (for/vector ([val row]
                     [type? types])
          (define ret-val (if type? ((hash-ref conversions type?) val) val))
          (if json? (ensure-json-value ret-val) ret-val)))]
    [(eq? ret-type hash)
     (λ (cols row json? types)
        (for/hash ([col cols]
                   [val row]
                   [type? types])
          (define ret-val (if type? ((hash-ref conversions type?) val) val))
          (define key (match case-type
                        ['snake (any->snake-case col)]
                        ['kebab (any->kebab-case col)]
                        ['camel (any->camel-case col)]
                        [_ col]))
          (values key (if json? (ensure-json-value ret-val) ret-val))))]))

(define (get-formatted-result cols types iter-func rows json?
                              #:custom-iter-func [custom-iter-func #f]
                              #:single-ret-val single-ret-val?
                              #:single-ret-val/f single-ret-val/f?
                              #:ret-type ret-type)
 (define result
   (for/list ([row rows])
     (if custom-iter-func
       (custom-iter-func cols row json? types)
       (iter-func cols row json? types))))
 (cond [(and (= (length rows) 0) single-ret-val/f?)
        #f]
       [(and (= (length rows) 0) single-ret-val?) (ret-type)]
       [(or single-ret-val? single-ret-val/f?)
        (car result)]
       [else result]))


; TODO add conversions for sql dates
(define (ensure-json-value val)
  (cond
    [(sql-null? val) 'null]
    [(integer? val) val]
    [(rational? val) (exact->inexact val)]
    [(list? val) (for/list ([el val]) (ensure-json-value el))]
    [(hash? val) (for/hash ([(k v) val]) (values
                                          (if (symbol? k) k
                                              (string->symbol (~a k)))
                                          (ensure-json-value v)))]
    [else val]))


(define (build-select-query [str ""]
                            #:columns [columns #f]
                            #:from [tables #f]
                            #:where [where #f]
                            #:order-by [order-by #f]
                            #:group-by [group-by #f]
                            #:limit [limit #f])
  (define (format-columns columns)
    (cond
      [(false? columns) columns]
      [(list? columns)
       (string-join
         (for/list ([col columns])
           (~a (if (list? col) (car col) col)))
         ", ")]
      [else (~a columns)]))
  (define (format-chunk sql chunk)
    (if chunk
      (format " ~a ~a" sql chunk)
      ""))
  (define query-string
    (format "~a~a~a~a~a~a~a"
            str
            (format-chunk "SELECT" (format-columns columns))
            (format-chunk "FROM" tables)
            (format-chunk "WHERE" where)
            (format-chunk "GROUP BY" group-by)
            (format-chunk "ORDER BY" order-by)
            (format-chunk "LIMIT" limit)))
  (string-trim
    (regexp-replace* #px"\\s{2,}"
                     (string-replace query-string "\n" " ") " "))) ; cleanup for logging purposes

(define (format-query-string-helper str-lst new-lst new-str current-len)
  (if (empty? str-lst)
    (if (equal? new-str "")
      new-lst
      (append new-lst (list (string-trim new-str))))
    (let ([str-len (string-length (car str-lst))])
      (cond
        [(>= str-len 70)
         (format-query-string-helper (cdr str-lst)
                                     (if (equal? new-str "")
                                       (append new-lst (list (string-trim (car str-lst))))
                                       (append new-lst (list new-str (string-trim (car str-lst)))))
                                     ""
                                     0)]
        [(>= (+ (string-length (car str-lst)) current-len) 70)
         (format-query-string-helper str-lst
                                     (append new-lst (list (string-trim new-str)))
                                     ""
                                     0)]
        [else (format-query-string-helper (cdr str-lst)
                                          new-lst
                                          (~a new-str " " (car str-lst))
                                          (+ 1 (string-length (car str-lst)) current-len))]))))

  (define (format-query-string str)
    (define split
      (regexp-match* #px"\\S+"
                     (string-replace (string-replace str "( " "(") " )" ")")))
    (format-query-string-helper split '() "" 0))

(define (get-datadef-mock-data dd position)
  (cond
    [(false? position) '()]
    [else
      (define pos (if (list? position) position (list position)))
      (for/list ([p pos])
        (for/vector ([val  dd])
          (define mock-data (caddr val))
          (if (list? mock-data)
            (list-ref mock-data p)
            mock-data)))]))
