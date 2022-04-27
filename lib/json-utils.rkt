#lang at-exp racket

(require db/base)

(provide ensure-json-value)


(define (ensure-leading-zero val)
  (~r val #:min-width 2 #:pad-string "0"))

(define (ensure-json-value val)
  (cond
    [(sql-null? val) 'null]
    [(symbol? val) (symbol->string val)]
    [(integer? val) val]
    [(rational? val) (exact->inexact val)]
    ; TODO add other formats than iso 8601?
    [(sql-date? val) (format "~a-~a-~a"
                             (sql-date-year val)
                             (ensure-leading-zero (sql-date-month val))
                             (ensure-leading-zero (sql-date-day val)))]
    [(sql-time? val) (apply format "~a:~a:~a"
                                  (map ensure-leading-zero (list (sql-time-hour val)
                                  (sql-time-minute val)
                                  (sql-time-second val))))]
    [(sql-timestamp? val) (apply format "~a-~a-~a ~a:~a:~a"
                                  (cons (sql-timestamp-year val)
                                        (map ensure-leading-zero
                                             (list (sql-timestamp-month val)
                                                   (sql-timestamp-day val)
                                                   (sql-timestamp-hour val)
                                                   (sql-timestamp-minute val)
                                                   (sql-timestamp-second val)))))]

    [(sql-interval? val) (sql-interval-days val)]
    [(vector? val) (ensure-json-value (vector->list val))]
    [(list? val) (for/list ([el val]) (ensure-json-value el))]
    [(hash? val) (for/hash ([(k v) val]) (values
                                          (if (symbol? k) k
                                              (string->symbol (~a k)))
                                          (ensure-json-value v)))]
    [else val]))

(module+ test
  (require rackunit
           json)
  (test-case
    "Conversion of list to jsexpr compatible"
    (check-true (jsexpr?
                  (map ensure-json-value
                       `(,sql-null 250/4 ())))))
  (test-case
    "Conversion of hash to jsexpr compatible"
    (check-true (jsexpr?
                  (ensure-json-value
                       (make-immutable-hash
                         `([,(make-immutable-hash `([x . 5]))]
                           [key1 . test]
                             [,sql-null . 1]
                             [key2 . #f]))))))
  (test-case
      "Conversion of vector to jsexpr compatible"
      (check-true (jsexpr?
                    (ensure-json-value #(1 2 3)))))
  (test-case
    "Conversion of sql times and dates"
    (check-true (jsexpr?
                  (ensure-json-value (sql-date 2020 02 20))))
    (check-true (jsexpr?
                  (ensure-json-value (sql-time 12 10 10 5 5))))
    (check-true (jsexpr?
                  (ensure-json-value (sql-timestamp 2020 02 20 12 10 10 5 5))))
    )
)
