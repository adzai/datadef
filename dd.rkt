#lang at-exp racket/base

(require (for-syntax racket/syntax
                     syntax/parse
                     racket/bool
                     racket/base)
         scribble/srcdoc
         (for-doc scribble/manual
                  scribble/core
                  scribble/html-properties
                  "lib/key-utils.rkt"
                  racket/base
                  racket/format
                  racket/string
                  "lib/utils.rkt")
         (only-in "dtb.rkt" db-mocking-data)
         racket/contract
         racket/function
         racket/string
         racket/format
         "lib/test-utils.rkt"
         "lib/key-utils.rkt"
         racket/list
         racket/bool
         "lib/utils.rkt")

(define datadef:db-rows-func (make-parameter #f))

(provide
  (form-doc
      (define-datadef name dd #:from from #:ret-type ret-type
                              [#:provide
                               #:single-ret-val
                               #:single-ret-val/f
                               #:keys-strip-prefix
                               #:camel-case
                               #:kebab-case
                               #:snake-case
                               #:where where
                               #:limit limit
                               #:order-by order-by
                               #:group-by group-by
                               doc-string])
      #:contracts ([name any/c]
                   [dd (listof any/c)]
                   [from string?]
                   [ret-type (or/c list vector hash)]
                   [where string?]
                   [limit integer?]
                   [order-by string?]
                   [group-by string?]
                   [doc-string string?])
      @{
        Creates a @racket[datadef] and a function @racket[datadef:name->result] to
        get result of @racket[dtb-query-rows] formatted with @racket[datadef-format-func]
        based on the provided @racket[#:ret-type].

        If @racket[#:provide] keyword is provided, scribble documentation gets created for the constant definition @racket[datadef:name] and the
        @racket[datadef:name->result] function. Both are exported as well.

        @racket[#:single-ret-val] can be used to return only 1 value. If there are no query results,
        it returns the empty version of the provided @racket[ret-type].


        @racket[#:single-ret-val/f] will return @racket[#f] if there are no query results.

        The last optional argument @racket[doc-string] can be used to provide additional documentation for the datadef
        with the usual scribble syntax.
      })
  define-conversion
  (parameter-doc
    datadef:db-rows-func
  (parameter/c
    (or/c false?
      (->i ((statement string?))
           () #:rest
           (rest (listof any/c))
           (result (listof vector?)))))
  db-rows-func
  @{
    Function used for retrieving data from the database.
  })

 ; Must be re-provided for syntax stage in order to use
 ; define/provide-datadef in racket/base modules
 (for-syntax quote #%datum)
 (parameter-doc
   datadef:ensure-json-func
   (parameter/c procedure?)
   datadef:ensure-json-func
   @{

   Parameter holding the function that will be used when
   @racket[#:json #t] is specified in the datadef result function.

   })
)

(define-syntax (define-datadef stx)
  (syntax-parse stx
    [(_ name dd (~or (~seq #:from from #:ret-type ret-type)
                          (~seq #:ret-type ret-type #:from from))
        (~seq
          (~or
            (~optional (~and #:single-ret-val single-kw))
            (~optional (~and #:single-ret-val/f single/f-kw))
            (~optional (~and #:provide provide?))
            (~optional (~and #:keys-strip-prefix keys-strip-prefix-kw))
            (~optional (~and #:kebab-case kebab-case?))
            (~optional (~and #:camel-case camel-case?))
            (~optional (~and #:snake-case snake-case?))
            (~optional (~seq #:limit limit)
                       #:defaults ([limit #'#f]))
            (~optional (~seq #:order-by order-by)
                       #:defaults ([order-by #'#f]))
            (~optional (~seq #:group-by group-by)
                       #:defaults ([group-by #'#f]))
            (~optional (~seq #:where where)
                       #:defaults ([where #'#f])))
          ...)
        (~optional doc-string #:defaults ([doc-string #'@{}])))
     (with-syntax* ([datadef:name (datum->syntax stx (string->symbol (format "datadef:~a" (syntax->datum #'name))))]
                    [ret-type-predicate (datum->syntax stx (string->symbol (format "~a?" (syntax->datum #'ret-type))))]
                    [ret-datum (syntax->datum #'ret-type)]
                    [single-ret-val? (if (attribute single-kw) #t #f)]
                    [single-ret-val/f? (if (attribute single/f-kw) #t #f)]
                    [case-thunk #`(cond
                                    [#,(not (false? (attribute kebab-case?))) any->kebab-case]
                                    [#,(not (false? (attribute camel-case?))) any->camel-case]
                                    [#,(not (false? (attribute snake-case?))) any->snake-case]
                                    [else (λ (key) key)])]
                    [keys-strip-prefix? (if (attribute keys-strip-prefix-kw) #t #f)]
                    [datadef-part-list #'(parse-datadef-parts dd case-thunk keys-strip-prefix?)]
                    [datadef-doc #'(map (λ (dp) (datadef-part-key dp)) datadef-part-list)]
                    [format-func-ret-type (if (or (attribute single-kw)
                                                  (attribute single/f-kw))
                                            (if (attribute single/f-kw) #'(or/c false? ret-type-predicate) #'ret-type-predicate)
                                            #'(listof ret-type-predicate))]
                    [result-func-contract #'(->i () (#:where [query-where (or/c false? string?)]
                                                                  #:order-by [query-order-by (or/c false? string?)]
                                                                  #:group-by [query-group-by (or/c false? string?)]
                                                                  #:limit [query-limit (or/c false? string?)]
                                                                  #:query-string-args [qs-args (listof any/c)]
                                                                  #:mutable [mutable boolean?]
                                                                  #:json [json? boolean?]) #:rest [query-args (listof any/c)]
                                                                  [result format-func-ret-type])]
                    [select-struct #'(make-sql-select #:columns dd
                                                      #:from from
                                                      #:where where
                                                      #:order-by order-by
                                                      #:group-by group-by
                                                      #:limit limit)]
                    [result-func-name (datum->syntax stx (string->symbol (format "datadef:~a->result" (syntax->datum #'name))))]
                    [query-string #'(build-select-query #:columns dd
                                                        #:from from
                                                        #:where where
                                                        #:order-by order-by
                                                        #:group-by group-by
                                                        #:limit limit)])
        (quasisyntax/loc stx
                         (begin
                           #,(if (attribute provide?)
                             #`(provide
                               (thing-doc
                                 datadef:name
                                 datadef?
                                 (#,@#'doc-string
                                  #,@#'{
                                  @paragraph[
                                             (style #f
                                                    (list (attributes
                                                            '((style . "text-decoration: underline;")))))
                                             "Query string"]
                                  @nested-flow[(make-style 'code-inset null) (map (λ (elem)
                                                                                     (paragraph (style "RktMeta" null)
                                                                                                elem))
                                                                                        (format-query-string query-string))]
                                  (let ([arg-len (length (regexp-match* #px"~a" query-string))])
                                    (if (> arg-len 0)
                                      (~a "Query string requires " arg-len " query-string-args arguments")
                                      ""))
                                  "Type: "
                                  @racket[format-func-ret-type]
                                  @paragraph[
                                             (style #f
                                                    (list (attributes
                                                            '((style . "text-decoration: underline;")))))
                                             "Keys"]
                                  @itemlist[(if (or (eq? ret-datum vector)
                                                    (eq? ret-datum list))
                                              (map item (for/list ([str-key (map ~a datadef-doc)]
                                                                   [num (length datadef-doc)])

                                                          (format "~a: ~a" num str-key)
                                                          ))
                                              (map item (map symbol->string datadef-doc)))]}))
                               (proc-doc
                                 result-func-name
                                 result-func-contract
                                 ([query-where #f]
                                  [query-order-by #f]
                                  [query-group-by #f]
                                  [query-limit #f]
                                  [qs-args empty]
                                  [mutable #f]
                                  [json #f])
                                 (#,@#'{
                                  "Executes the query from " @racket[datadef:name] " and returns the resulting data structure. "
                                  "The function accepts additional arguments to extend " @racket[datadef-query-string]
                                  " such as " @racket[#:where] ", " @racket[#:order-by] ", " @racket[#:group-by] ", " @racket[#:limit] ". "
                                  @racket[query-string-args] " accepts a list of argument for missing parameters from the query-string
                                  itself that are represented with the placeholder ~a."})))
                                  #'(void))
                       (define/contract (result-func-name #:where [query-where #f]
                                                 #:order-by [query-order-by #f]
                                                 #:group-by [query-group-by #f]
                                                 #:limit [query-limit #f]
                                                 #:query-string-args [qs-args '()]
                                                 #:mutable [mutable #f]
                                                 #:json [json? #f]
                                                 #:custom-iter-func [custom-iter-func #f]
                                                 . query-args)
                          result-func-contract
                         (define qs (cond
                                      [(or query-where query-order-by query-group-by query-limit)
                                       (define select (datadef-sql-select datadef:name))
                                       (define new-where (or query-where (sql-select-where select)))
                                       (define new-order-by (or query-order-by (sql-select-order-by select)))
                                       (define new-group-by (or query-group-by (sql-select-group-by select)))
                                       (define new-limit (or query-limit (sql-select-limit select)))
                                       (build-select-query #:columns dd
                                                           #:from from
                                                           #:where new-where
                                                           #:order-by new-order-by
                                                           #:group-by new-group-by
                                                           #:limit new-limit)]
                                      [else (datadef-query-string datadef:name)]))
                         (define final-query-string (cond
                                                      [(string? qs-args)
                                                       (format qs qs-args)]
                                                      [(not (empty? qs-args))
                                                       (apply format qs qs-args)]
                                                      [else qs]))
                         (when (and (db-mocking-data)
                                    (hash-ref (db-mocking-data) 'datadef #f)
                                    (false? (hash-has-key? (db-mocking-data) (syntax->datum #'datadef:name))))
                           (define positions (range (length (datadef-part-mock-data (car (datadef-parts datadef:name))))))
                           (if (empty? positions)
                             (error "No mock data defined")
                             (hash-set! (db-mocking-data) (syntax->datum #'datadef:name) (db-mock #f positions))))
                         (define dtb-ret
                           (cond
                             [(and (db-mocking-data)
                                   (hash-has-key? (db-mocking-data) (syntax->datum #'datadef:name)))
                              (define mock (hash-ref (db-mocking-data) (syntax->datum #'datadef:name)))
                              (define positions (db-mock-positions mock))
                              (define pos (cond
                                            [(list? positions)
                                            (car positions)]
                                            [(void? positions) '(0)]
                                            [(false? positions) #f]
                                            [else (list positions)]))
                              (if (db-mock-data mock)
                                (let ([data (car (db-mock-data mock))]
                                      [rest-data (cdr (db-mock-data mock))])
                                  (unless (void? positions)
                                    (hash-set! (db-mocking-data) (syntax->datum #'datadef:name) (db-mock rest-data (if (and (list? positions)
                                                                                                                                (not (empty? positions)))
                                                                                                                          (remove pos positions)
                                                                                                                          positions))))
                                  (get-mock-data rest-data pos))
                                (begin
                                  (unless (void? positions)
                                    (hash-set! (db-mocking-data) (syntax->datum #'datadef:name) (db-mock (db-mock-data mock) (if (and (list? positions)
                                                                                                                                (not (empty? positions)))
                                                                                                                          (remove pos positions)
                                                                                                                          positions))))
                                  (get-mock-data (datadef-parts datadef:name) pos)))
                              ]
                             [(datadef:db-rows-func)
                              (apply
                                (datadef:db-rows-func)
                                final-query-string
                                query-args)]
                             [else (error "mock data nor datadef:db-rows-func set")]))
                         (define ret
                           ((curry get-formatted-result datadef-part-list (get-iter-func ret-datum)
                                   #:single-ret-val single-ret-val?
                                   #:single-ret-val/f single-ret-val/f?
                                   #:ret-type ret-datum) dtb-ret json? #:custom-iter-func custom-iter-func))
                         (if mutable
                           (if (list? ret)
                             (if (and (not (empty? ret))
                                      (hash? (car ret)))
                               (map hash-copy ret) ret)
                             (if (hash? ret) (hash-copy ret) ret))
                           ret))
                       (define datadef:name
                         (datadef datadef-part-list query-string result-func-name
                                  select-struct)))))]))

(module+ test
  (require rackunit
           (only-in "lib/json-utils.rkt" ensure-json-value)
           db
           "dtb.rkt")
  (test-case
    "Basic datadef"
  (define-datadef test
  '(column1 column2)
    #:ret-type hash
    #:from "table")
  (check-pred datadef? datadef:test)
  (check-equal? (datadef-part-col (cadr (datadef-parts datadef:test))) 'column2)
  (check-equal? (datadef-query-string datadef:test) "SELECT column1, column2 FROM table"))
  (test-case
      "Datadef with query kwargs"
    (define-datadef test
     '(table.column_key)
      #:ret-type hash
      #:from "table"
      #:camel-case
      #:keys-strip-prefix
      #:where "x=1")
    (check-pred datadef? datadef:test)
    (check-equal? (datadef-part-key (car (datadef-parts datadef:test))) 'columnKey)
    (check-equal? (datadef-query-string datadef:test) "SELECT table.column_key FROM table WHERE x=1"))
  (test-case
      "list of hash and empty list return type"
      (define-datadef test
                      '((column1 _ (val1 val2)) (column2 _ (val3 val4)))
                      #:ret-type hash
                      #:from "table")
      (with-mock-data ((datadef:test ((0 1) #f)))
        ; List of hash
        (check-equal? (datadef:test->result)
                      `(,#hash([column1 . val1]
                               [column2 . val3])
                         ,#hash([column1 . val2]
                                [column2 . val4])))
        ; Empty list
        (check-equal? (datadef:test->result)
                      '())))
    (test-case
      "hash and empty hash return type"
      (define-datadef test
                      '((column1 _ (val1)) (column2 _ (val2)))
                      #:ret-type hash
                      #:from "table"
                      #:single-ret-val)
           (with-mock-data ((datadef:test (0 #f)))
        ; Hash
        (check-equal? (datadef:test->result)
                      #hash([column1 . val1]
                            [column2 . val2]))
        ; Empty hash
        (check-equal?
          (datadef:test->result)
          (hash))))
    (test-case
      "#f return type"
      (define-datadef test
                      '(column1)
                      #:ret-type hash
                      #:from "table"
                      #:single-ret-val/f)
      (with-mock-data ((datadef:test (#f)))
        ; #f
        (check-false (datadef:test->result))))
    (test-case
      "list of lists and empty list return type"
      (define-datadef test
                      '((column1 _ (val1 val2)) (column2 _ (val3 val4)))
                      #:ret-type list
                      #:from "table")
      (with-mock-data ((datadef:test ((0 1) #f)))
        ; List of lists
        (check-equal?
          (datadef:test->result)
          '((val1 val3) (val2 val4)))
        ; Empty list
        (check-equal?
          (datadef:test->result)
          '())))
    (test-case
      "list of vectors and empty list return type"
      (define-datadef test
                      '((column1 _ (val1 val2)) (column2 _ (val3 val4)))
                      #:ret-type vector
                      #:from "table")
      (with-mock-data ((datadef:test ((0 1) #f)))
        ; List of vectors
        (check-equal?
          (datadef:test->result)
          `(,#(val1 val3) ,#(val2 val4)))
        ; Empty list
        (check-equal?
          (datadef:test->result)
          '())))
    (test-case
      "keys-strip-prefix keyword"
      (define-datadef test
                      '((table.column1 _ (val1)) (table.column2 _ (val2)))
                      #:ret-type hash
                      #:from "table"
                      #:single-ret-val
                      #:keys-strip-prefix)
      (with-mock-data ((datadef:test  (0)))
        (check-equal? (hash-keys (datadef:test->result) #t)
                      '(column1 column2))))
    (test-case
      "kebab case"
      (define-datadef test
                      '((snake_column1 _ (val1)) (snake_column2 _ (val2)))
                      #:ret-type hash
                      #:from "table"
                      #:single-ret-val
                      #:kebab-case)
      (with-mock-data ((datadef:test (0)))
        (check-equal? (hash-keys (datadef:test->result) #t)
                      '(snake-column1 snake-column2))))
    (test-case
      "snake case"
      (define-datadef test
                      '((snake-column1 _ (val1)) (snake-column2 _ (val2)))
                      #:ret-type hash
                      #:from "table"
                      #:single-ret-val
                      #:snake-case)
      (with-mock-data ((datadef:test (0)))
        (check-equal? (hash-keys (datadef:test->result) #t)
                      '(snake_column1 snake_column2))))
    (test-case
      "camel case"
      (define-datadef test
                      '((snake-column1 _ (val1)) (snake-column2 _ (val2)))
                      #:ret-type hash
                      #:from "table"
                      #:single-ret-val
                      #:camel-case)
      (with-mock-data ((datadef:test . (0)))
        (check-equal? (hash-keys (datadef:test->result) #t)
                      '(snakeColumn1 snakeColumn2))))
    (test-case "#:datadef"
      (define-datadef test
                          '((column1 _ val1) (column2 _ val2))
                          #:ret-type hash
                          #:from "table")
      (with-mock-data #:datadef
        (check-equal? (datadef:test->result)
                      `(,#hash([column1 . val1]
                               [column2 . val2])))))
    (test-case "ensure json parameter"
      (define-datadef test
                      '((column1 _ val1))
                      #:ret-type hash
                      #:from "table"
                      #:single-ret-val/f)
      (check-equal? (datadef:ensure-json-func) ensure-json-value) ; Default function
      (parameterize ([datadef:ensure-json-func (λ (val) "Custom func")])
        (with-mock-data #:datadef
                        (check-equal? (datadef:test->result #:json #t)
                                      #hash([column1 . "Custom func"])))))
)
