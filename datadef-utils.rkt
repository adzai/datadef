#lang at-exp racket/base

(require scribble/srcdoc
         db/base
         (for-syntax racket/base)
         racket/bool
         racket/contract
         racket/list
         racket/string
         racket/format)

(provide
  build-select-query
  format-query-string
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
    (->i ((cols (listof symbol?)) (iter-func (-> (listof symbol?) vector? boolean? (or/c vector? list? hash?)))
                                  (rows (listof vector?)) (json? boolean?)
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
     (-> procedure? (-> (listof symbol?) vector? boolean? (or/c vector? list? hash?)))
     (ret-type)
     @{
      Returns appropriate function for iterating over db rows based on the given
      datadef's return type.
     }
   )
)

(define (get-datadef-key col)
  (cond
    [(list? col)
     (define key (cadr col))
     (if (eq? '_ key) (car col) key)]
    [else col]))

(define (datadef->keys datadef #:doc [doc #f])
  (for/list ([dd datadef])
    (cond
      [(symbol? dd)
       (define str-lst
         (string-split
           (symbol->string dd)
           "."))
       (string->symbol
         (if (> (length str-lst) 1)
           (get-datadef-key str-lst)
           (car str-lst)))]
      [(list? dd)
       (cadr dd)]
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

(define (get-iter-func ret-type)
  (cond
    [(eq? ret-type list)
     (λ (cols row json?)
        (for/list ([val row])
          (if json? (ensure-json-value val)
            val)))]
    [(eq? ret-type vector)
     (λ (cols row json?)
        (for/vector ([val row])
          (if json? (ensure-json-value val)
            val)))]
    [(eq? ret-type hash)
     (λ (cols row json?)
        (for/hash ([col cols]
                   [val row])
          (values col (if json? (ensure-json-value val)
                        val))))]))

(define (get-formatted-result cols iter-func rows json?
                              #:custom-iter-func [custom-iter-func #f]
                              #:single-ret-val single-ret-val?
                              #:single-ret-val/f single-ret-val/f?
                              #:ret-type ret-type)
 (define result
   (for/list ([row rows])
     (if custom-iter-func
       (custom-iter-func cols row json?)
       (iter-func cols row json?))))
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
                            #:limit [limit #f]
                            #:sub-alias [sub-alias #f])
  (when (and (not (equal? str ""))
             (or columns tables sub-alias))
    (cond
      [columns (error "Query must already contain #:columns if initial string is provided")]
      [tables (error "Query must already contain #:from if initial string is provided")]
      [columns (error "Sub queries are not supported when initial string is provided")]))
  (when (and (equal? str "")
             (or (not columns)
                 (not tables)))
    (error "Must provide #:columns and #:from if initial string is not provided"))

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
  (define result
    (if sub-alias (format "(~a) ~a" query-string sub-alias)
      query-string))
  (string-trim
    (regexp-replace* #px"\\s{2,}"
                     (string-replace result "\n" " ") " "))) ; cleanup for logging purposes

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
