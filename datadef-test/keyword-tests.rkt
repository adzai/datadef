#lang racket

(module+ test
  (require rackunit
           "../dtb-module.rkt"
           "../datadef.rkt")
  (test-case
    "Testing list of hash and empty list return type"
    (define-datadef test
                    '((column1 _ (val1 val2)) (column2 _ (val3 val4)))
                    #:ret-type hash
                    #:from "table")
    (parameterize ([db-mocking-data #hash([datadef:test . ((0 1) #f)])])
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
    "Testing hash and empty hash return type"
    (define-datadef test
                    '((column1 _ (val1)) (column2 _ (val2)))
                    #:ret-type hash
                    #:from "table"
                    #:single-ret-val)
    (parameterize ([db-mocking-data #hash([datadef:test . (0 #f)])])
      ; Hash
      (check-equal? (datadef:test->result)
                    #hash([column1 . val1]
                          [column2 . val2]))
      ; Empty hash
      (check-equal?
        (datadef:test->result)
        (hash))))
  (test-case
    "Testing #f return type"
    (define-datadef test
                    '(column1)
                    #:ret-type hash
                    #:from "table"
                    #:single-ret-val/f)
    (parameterize ([db-mocking-data #hash([datadef:test . (#f)])])
      ; #f
      (check-false (datadef:test->result))))
  (test-case
    "Testing list of lists and empty list return type"
    (define-datadef test
                    '((column1 _ (val1 val2)) (column2 _ (val3 val4)))
                    #:ret-type list
                    #:from "table")
    (parameterize ([db-mocking-data #hash([datadef:test . ((0 1) #f)])])
      ; List of lists
      (check-equal?
        (datadef:test->result)
        '((val1 val3) (val2 val4)))
      ; Empty list
      (check-equal?
        (datadef:test->result)
        '())))
  (test-case
    "Testing list of vectors and empty list return type"
    (define-datadef test
                    '((column1 _ (val1 val2)) (column2 _ (val3 val4)))
                    #:ret-type vector
                    #:from "table")
    (parameterize ([db-mocking-data #hash([datadef:test . ((0 1) #f)])])
      ; List of vectors
      (check-equal?
        (datadef:test->result)
        `(,#(val1 val3) ,#(val2 val4)))
      ; Empty list
      (check-equal?
        (datadef:test->result)
        '())))
  (test-case
      "Testing keys-strip-prefix keyword"
      (define-datadef test
                      '((table.column1 _ (val1)) (table.column2 _ (val2)))
                      #:ret-type hash
                      #:from "table"
                      #:single-ret-val
                      #:keys-strip-prefix) ; TODO handle datadef->keys and columns->keys
      (parameterize ([db-mocking-data #hash([datadef:test . (0)])])
        (check-equal? (hash-keys (datadef:test->result) #t)
                      '(column1 column2))))
)
