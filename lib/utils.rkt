#lang at-exp racket/base

(require scribble/srcdoc
         scribble/core
         (for-syntax racket/base)
         (for-doc scribble/manual)
         racket/bool
         "key-utils.rkt"
         "json-utils.rkt"
         racket/contract
         racket/match
         racket/list
         racket/string
         racket/format)

(provide
  (struct-out sql-select)
  make-sql-select
  (struct*-doc
      datadef
      ([parts (listof datadef-part?)]
       [query-string string?]
       [result-func (->i () (#:where [query-where (or/c false? string?)]
                             #:order-by [query-order-by (or/c false? string?)]
                             #:group-by [query-group-by (or/c false? string?)]
                             #:limit [query-limit (or/c false? string?)]
                             #:query-string-args [qs-args (listof any/c)]
                             #:mutable [mutable boolean?]
                             #:json [json? boolean?]) #:rest [query-args (listof any/c)]
                         [result (or/c list? vector? hash? false?)])]
       [sql-select sql-select?])
     #:transparent
     @{

     Datadef @racket[struct] holding information about a datadef with functions to
     query the database and return a properly formatted data structure.

     })
  (struct*-doc
      datadef-part
      ([col (or/c symbol? string?)]
       [key symbol?]
       [mock-data (or/c false? list?)]
       [type (or/c symbol? false?)])
     #:transparent
     @{})
  (struct-out db-mock)
  (proc-doc/names
    parse-datadef-parts
    (-> list? (-> symbol? symbol?) boolean? (listof datadef-part?))
  (list-of-dd case-thunk keys-strip-prefix?)
  @{})
  (form-doc
    (define-conversion predicate procedure)
    #:contracts ([predicate symbol?]
                 [procedure (-> any/c any)])
    @{})
  (proc-doc/names
      build-select-query
    (->* []
         [string?
          #:columns (or/c string? (listof any/c))
          #:from (or/c false? string?)
          #:where (or/c false? string?)
          #:order-by (or/c false? string?)
          #:group-by (or/c false? string?)
          #:limit (or/c false? integer?)]
         string?)
    ([]
     ([str ""]
      [columns #f]
      [tables #f]
      [where #f]
      [order-by #f]
      [group-by #f]
      [limit #f]))
    @{
    Creates a query @racket[string] based on provided arguments.
    @racket[#:columns] and @racket[#:from] must be provided if initial string
    (@racket[str]) is not supplied.
    If @racket[str] is provided, @racket[#:columns] and @racket[#:from] must
    not be supplied.

    @racket[#:columns] can be a string or a datadef.
  })
  (proc-doc/names
      format-query-string
      (-> string? (listof string?))
      (str)
      @{
      Accepts a query string and returns a list of strings, which can be used in scribble
      documentation.
  })
  (proc-doc
    get-formatted-result
    (->i ((datadef-part-list (listof datadef-part?))
          (iter-func (-> (listof datadef-part?) vector? boolean? (or/c vector? list? hash?)))
          (rows (listof vector?))
          (json? boolean?)
          #:single-ret-val (single-ret-val boolean?)
          #:single-ret-val/f (single-ret-val/f boolean?)
          #:ret-type (ret-type procedure?))
         (#:custom-iter-func (custom-iter-func (or/c false?(-> (listof datadef-part?) vector? boolean? (or/c vector? list? hash?)))))
         (result (or/c list? hash? vector? false?)))
    (#f)
    @{
      Formats db rows and returns a result based on the datadef's return type.
    }
  )
  (proc-doc/names
     get-iter-func
     (-> procedure? (-> (listof datadef-part?) vector? boolean? (or/c vector? list? hash?)))
     (ret-type)
     @{
      Returns appropriate function for iterating over db rows based on the given
      datadef's return type.
     }
   )
  (proc-doc/names
       get-mock-data
       (-> (or/c (listof datadef-part?) (listof any/c))
           (or/c (listof (or/c false? integer?))
                 (or/c false? integer?)) any)
      (datadef-part-list position)
      @{})
  (parameter-doc
    datadef:ensure-json-func
    (parameter/c procedure?)
    datadef:ensure-json-func
    @{

    Parameter holding the function that will be used when
    @racket[#:json #t] is specified in the datadef result function.

    })
)

(struct datadef (parts query-string result-func sql-select) #:transparent)
(struct datadef-part (col key mock-data type) #:transparent)
(struct db-mock (data positions) #:transparent)

(define datadef:ensure-json-func (make-parameter ensure-json-value))

(define (parse-datadef-parts list-of-dd case-thunk keys-strip-prefix?)
  (define strip-prefix (if keys-strip-prefix?
                          (λ (key) (string->symbol
                                      (string-join (cdr (string-split (~a key) ".")) ".")))
                          (λ (key) key)))
  (for/list ([dd list-of-dd])
    (cond
      [(and (list? dd) (not (empty? dd)))
       (define len (length dd))
       (define 1st (car dd))
       (define 2nd (case-thunk
                     (strip-prefix
                       (if (and (>= len 2)
                                (not (eq? (cadr dd) '_)))
                         (cadr dd) 1st))))
       (define 3rd (if (>= len 3) (let ([val (caddr dd)]) (if (list? val) val (list val))) #f))
       (define 4th (if (= len 4) (cadddr dd) #f))
       (datadef-part 1st 2nd 3rd 4th)]
    [(symbol? dd) (datadef-part dd (case-thunk (strip-prefix dd)) #f #f)]
    [else (error (format "Expected list of a symbol, got ~v" dd))])))

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

; TODO add to docs that type conversion has precedence over auto json conversion
(define (ensure-type/json type? json? val)
  (cond
    [type? ((hash-ref conversions type?) val)]
    [json? ((datadef:ensure-json-func) val)]
    [else val]))

(define (get-iter-func ret-type)
  (cond
    [(eq? ret-type list)
     (λ (dp-list row json?)
        (for/list ([val row]
                   [dp dp-list])
          (ensure-type/json (datadef-part-type dp) json? val)))]
    [(eq? ret-type vector)
     (λ (dp-list row json?)
        (for/vector ([val row]
                     [dp dp-list])
          (ensure-type/json (datadef-part-type dp) json? val)))]
    [(eq? ret-type hash)
     (λ (dp-list row json?)
        (for/hash ([val row]
                   [dp dp-list])
          (values (datadef-part-key dp)
                  (ensure-type/json (datadef-part-type dp) json? val))))]))

(define (get-formatted-result datadef-part-list iter-func rows json?
                              #:custom-iter-func [custom-iter-func #f]
                              #:single-ret-val single-ret-val?
                              #:single-ret-val/f single-ret-val/f?
                              #:ret-type ret-type)
 (define result
   (for/list ([row rows])
     (if custom-iter-func
       (custom-iter-func datadef-part-list row json?)
       (iter-func datadef-part-list row json?))))
 (cond [(and (= (length rows) 0) single-ret-val/f?)
        #f]
       [(and (= (length rows) 0) single-ret-val?) (ret-type)]
       [(or single-ret-val? single-ret-val/f?)
        (car result)]
       [else result]))

(define (get-mock-data data-list position)
  (cond
    [(false? position) '()]
    [else
      (define pos (if (list? position) position (list position)))
      (for/list ([p pos])
        (for/vector ([part data-list])
          (define mock-data (if (datadef-part? part) (datadef-part-mock-data part) part))
          (if (list? mock-data)
            (list-ref mock-data p)
            mock-data)))]))

(struct sql-select (columns from where order-by group-by limit) #:transparent)

(define (make-sql-select #:columns [columns #f]
                         #:from [from #f]
                         #:where [where #f]
                         #:order-by [order-by #f]
                         #:group-by [group-by #f]
                         #:limit [limit #f])
  (sql-select columns from where order-by group-by limit))

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
