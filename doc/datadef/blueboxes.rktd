796
((3) 0 () 3 ((q lib "datadef/datadef-lib/utils.rkt") (q lib "datadef/main.rkt") (q 0 . 12)) () (h ! (equal) ((c def c (c (? . 1) q datadef-query-string)) c (? . 2)) ((c def c (c (? . 0) q get-iter-func)) q (3147 . 5)) ((c def c (c (? . 0) q datadef->columns)) q (1935 . 3)) ((c def c (c (? . 0) q get-formatted-result)) q (2008 . 21)) ((c def c (c (? . 1) q datadef?)) c (? . 2)) ((c def c (c (? . 0) q columns->keys)) q (1813 . 4)) ((c def c (c (? . 1) q datadef)) c (? . 2)) ((c def c (c (? . 1) q db-mocking-data)) q (1654 . 2)) ((c form c (c (? . 1) q define-datadef)) q (560 . 24)) ((c def c (c (? . 1) q datadef-format-func)) c (? . 2)) ((c def c (c (? . 0) q datadef->keys)) q (1691 . 4)) ((c def c (c (? . 1) q struct:datadef)) c (? . 2)) ((c def c (c (? . 1) q datadef-dd)) c (? . 2))))
struct
(struct datadef (dd query-string format-func)
    #:transparent)
  dd : (or/c symbol? (list/c (or/c symbol? string?)
                          symbol?
                          (or/c any/c (listof any/c))
                          symbol?))
  query-string : string?
  format-func : (->* [(or/c (listof any/c)
                                  vector?)
                      boolean?]
                      (or/c list? vector? hash?))
syntax
(define-datadef name dd #:from from #:ret-type ret-type
                        [#:provide
                         #:single-ret-val
                         #:single-ret-val/f
                         #:keys-strip-prefix
                         #:camel-case
                         #:kebab-case
                         #:snake-case
                         #:where where
                         #:limit limit
                         #:order-by order-by
                         #:group-by group-by
                         doc-string])
 
  name : any/c
  dd : (listof any/c)
  from : string?
  ret-type : (or/c list vector hash)
  where : string?
  limit : integer?
  order-by : string?
  group-by : string?
  doc-string : string?
value
db-mocking-data : parameter?
procedure
(datadef->keys datadef [#:doc doc]) -> (listof symbol?)
  datadef : list?
  doc : boolean? = #f
procedure
(columns->keys columns [#:doc doc]) -> (listof symbol?)
  columns : list?
  doc : boolean? = #f
procedure
(datadef->columns datadef) -> string?
  datadef : list?
procedure
(get-formatted-result  cols                                  
                       types                                 
                       iter-func                             
                       rows                                  
                       json?                                 
                       #:single-ret-val single-ret-val       
                       #:single-ret-val/f single-ret-val/f   
                       #:ret-type ret-type                   
                      [#:custom-iter-func custom-iter-func]) 
 -> (or/c list? hash? vector? false?)
  cols : (listof symbol?)
  types : (listof (or/c symbol? false?))
  iter-func : (-> (listof symbol?) vector? boolean? (listof (or/c symbol? false?)) (or/c vector? list? hash?))
  rows : (listof vector?)
  json? : boolean?
  single-ret-val : boolean?
  single-ret-val/f : boolean?
  ret-type : procedure?
  custom-iter-func : (or/c false? (-> (listof symbol?) vector? boolean? (or/c vector? list? hash?)))
                   = #f
procedure
(get-iter-func ret-type case-type)
 -> (-> (listof symbol?) vector? boolean? (listof (or/c symbol? false?)) (or/c vector? list? hash?))
  ret-type : procedure?
  case-type : symbol?
