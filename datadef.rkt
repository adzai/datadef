#lang at-exp racket/base

(require (for-syntax racket/syntax
                     syntax/parse
                     racket/base)
         scribble/srcdoc
         (for-doc scribble/manual
                  scribble/core
                  scribble/html-properties
                  racket/base
                  racket/format
                  racket/string
                  "datadef-utils.rkt")
         racket/contract
         racket/function
         racket/list
         racket/bool
         (only-in "dtb-module.rkt" db-mocking-data)
         "datadef-utils.rkt")

(define datadef-db-rows-func (make-parameter #f))

(provide
  define-conversion
  datadef-db-rows-func

 ; Must be re-provided for syntax stage in order to use
 ; define/provide-datadef in racket/base modules
 (for-syntax quote #%datum)
 
  (struct*-doc
    datadef
    ([dd (or/c symbol? (list/c (or/c symbol? string?)
                            symbol?
                            (or/c any/c (listof any/c))
                            symbol?))]
     [query-string string?]
     [format-func (->* [(or/c (listof any/c)
                                    vector?)
                        boolean?]
                        (or/c list? vector? hash?))])
   #:transparent
   @{

   Datadef @racket[struct] holding information about a datadef with functions to
   query the database and return a properly formatted data structure.

   })
  (form-doc
    (define-datadef name dd #:from from #:ret-type ret-type
                            [#:provide
                             #:single-ret-val
                             #:single-ret-val/f
                             #:keys-strip-prefix
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
  )

(struct datadef (dd query-string format-func) #:transparent)

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
                    [datadef-keys-func (if (attribute keys-strip-prefix-kw) #'datadef->keys #'columns->keys)]
                    [datadef-doc #'(datadef-keys-func dd #:doc #t)]
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
                                  @paragraph[
                                             (style #f
                                                    (list (attributes
                                                            '((style . "text-decoration: underline;")))))
                                             "Keys" ]
                                  "Type: "
                                  @racket[format-func-ret-type]
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
                                  #''())
                       (define datadef:name
                         (datadef dd query-string (curry get-formatted-result (datadef-keys-func dd) (datadef->types dd) (get-iter-func ret-datum)
                                                                     #:single-ret-val single-ret-val?
                                                                     #:single-ret-val/f single-ret-val/f?
                                                                     #:ret-type ret-datum)))
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
                         (define qs (if (or query-where query-order-by query-group-by query-limit)
                                      (build-select-query query-string
                                                          #:where query-where
                                                          #:order-by query-order-by
                                                          #:group-by query-group-by
                                                          #:limit query-limit)
                                      query-string))
                         (define final-query-string (cond
                                                      [(string? qs-args)
                                                       (format qs qs-args)]
                                                      [(not (empty? qs-args))
                                                       (apply format qs qs-args)]
                                                      [else qs]))
                         (define dtb-ret
                           (cond
                             [(and (db-mocking-data)
                                   (member (syntax->datum #'datadef:name) (hash-keys (db-mocking-data))))
                              (define positions (hash-ref (db-mocking-data) (syntax->datum #'datadef:name)))
                              (define pos (if (list? positions)
                                            (car positions)
                                            (list positions)))
                              (when (immutable? (db-mocking-data)) (db-mocking-data (hash-copy (db-mocking-data))))
                              (hash-set! (db-mocking-data) (syntax->datum #'datadef:name) (remove pos positions))
                              (get-datadef-mock-data datadef:name pos)]
                             [else (apply
                                     (datadef-db-rows-func)
                                     final-query-string
                                     query-args)]))
                         (define ret ((curry get-formatted-result (datadef-keys-func dd) (datadef->types dd) (get-iter-func ret-datum)
                                                                     #:single-ret-val single-ret-val?
                                                                     #:single-ret-val/f single-ret-val/f?
                                                                     #:ret-type ret-datum) dtb-ret json? #:custom-iter-func custom-iter-func))
                         (if mutable
                           (if (list? ret)
                             (map hash-copy ret)
                             (hash-copy ret))
                           ret)))))]))

(define (get-datadef-mock-data dd position)
  (define pos (if (list? position) position (list position)))
  (for/list ([p pos])
    (for/vector ([val (datadef-dd dd)])
          (define mock-data (caddr val))
          (if (list? mock-data)
            (list-ref mock-data p)
            mock-data))))

(module+ test
  (require rackunit)
  (test-case
    "Basic datadef"
  (define-datadef test
  '(column1 column2)
    #:ret-type hash
    #:from "table")
  (check-pred datadef? datadef:test)
  (check-equal? (datadef-dd datadef:test) '(column1 column2))
  (check-equal? (datadef-query-string datadef:test) "SELECT column1, column2 FROM table"))
  (test-case
      "Datadef with query kwargs"
    (define-datadef test
     '(column1)
      #:ret-type hash
      #:from "table"
      #:where "x=1")
    (check-pred datadef? datadef:test)
    (check-equal? (datadef-dd datadef:test) '(column1))
    (check-equal? (datadef-query-string datadef:test) "SELECT column1 FROM table WHERE x=1"))
  (test-case
    "Testing return types"
    ; TODO accept datadef, allow different type?
    (define-datadef test
    '((column1 _ (val1)) (column2 _ (val2)))
      #:ret-type hash
      #:from "table")
    (parameterize ([db-mocking-data #hash([datadef:test . (0)])])
      ; List of hash
      (check-equal? (datadef:test->result)
                    (list #hash([column1 . val1]
                                [column2 . val2])))
                    ))
  )
