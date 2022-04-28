#lang racket

(module+ test
  (require rackunit
           "../main.rkt")
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
)
