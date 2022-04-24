#lang racket

(define (any->camel-case* char-lst)
  (if (empty? char-lst)
    '()
      (if (or (char=? (car char-lst) #\_)
              (char=? (car char-lst) #\-))
         (if (empty? (cdr char-lst))
           '()
           (cons (char-upcase (cadr char-lst))
                 (any->camel-case* (cddr char-lst))))
         (cons (car char-lst)
               (any->camel-case* (cdr char-lst))))))


(define/contract (any->camel-case key)
  (-> string? string?)
  (define char-lst (string->list key))
  (list->string
    (if (empty? char-lst)
      '()
      (let loop ([current-char (car char-lst)]
                 [rest-chars (cdr char-lst)])
              (cond
                [(char-upper-case? current-char)
                 (cons (char-downcase current-char) (any->camel-case* rest-chars))]
                [(char-alphabetic? current-char) (cons current-char (any->camel-case* rest-chars))]
                [else (loop (cadr char-lst) (cdr rest-chars))]))))
)


(module+ test
  (require rackunit)
  (test-case
    "Conversion to camel case"
    (check-equal? (any->camel-case "_snake_case")
                  "snakeCase")
    (check-equal? (any->camel-case "snake_case")
                  "snakeCase")
    (check-equal? (any->camel-case "case_")
                  "case")
    (check-equal? (any->camel-case "camelCase")
                  "camelCase")
    (check-equal? (any->camel-case "PascalCase")
                  "pascalCase")
    (check-equal? (any->camel-case "_PascalCase")
                  "pascalCase")
    (check-equal? (any->camel-case "-kebab-case")
                  "kebabCase")
    (check-equal? (any->camel-case "kebab-case")
                  "kebabCase")
    )
)
