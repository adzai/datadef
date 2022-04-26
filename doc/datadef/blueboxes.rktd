795
((3) 0 () 3 ((q lib "datadef/dd.rkt") (q 0 . 12) (q lib "datadef/lib/utils.rkt")) () (h ! (equal) ((c def c (c (? . 0) q datadef)) c (? . 1)) ((c def c (c (? . 0) q datadef?)) c (? . 1)) ((c def c (c (? . 0) q datadef-dd)) c (? . 1)) ((c def c (c (? . 2) q get-iter-func)) q (3332 . 5)) ((c def c (c (? . 2) q get-formatted-result)) q (2193 . 21)) ((q form ((lib "datadef/dtb.rkt") db-funcs-init)) q (1654 . 6)) ((c def c (c (? . 2) q datadef->keys)) q (1876 . 4)) ((c def c (c (? . 2) q datadef->columns)) q (2120 . 3)) ((c def c (c (? . 0) q struct:datadef)) c (? . 1)) ((c def c (c (? . 2) q columns->keys)) q (1998 . 4)) ((c def c (c (? . 0) q datadef-query-string)) c (? . 1)) ((c def c (c (? . 0) q datadef-format-func)) c (? . 1)) ((c form c (c (? . 0) q define-datadef)) q (560 . 24))))
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
syntax
(db-funcs-init prefix #:connection-func conn-func
                     [#:exn-fail-thunk exn-fail-thunk])
 
  prefix : any/c
  connection-func : (-> connection-pool? connection?)
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
