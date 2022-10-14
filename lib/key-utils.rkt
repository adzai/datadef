#lang racket

(provide any->camel-case
         any->kebab-case
         any->snake-case
         prefix-dot->underscore)

(define (any->camel-case* char-lst)
  (if (empty? char-lst)
    '()
      (if (or (char=? (car char-lst) #\_)
              (char=? (car char-lst) #\-)
              (char=? (car char-lst) #\.))
         (if (empty? (cdr char-lst))
           '()
           (cons (char-upcase (cadr char-lst))
                 (any->camel-case* (cddr char-lst))))
         (cons (car char-lst)
               (any->camel-case* (cdr char-lst))))))

(define/contract (any->camel-case key)
  (-> symbol? symbol?)
  (define char-lst (string->list (symbol->string key)))
  (string->symbol
    (list->string
      (if (empty? char-lst)
        '()
        (let loop ([current-char (car char-lst)]
                   [rest-chars (cdr char-lst)])
          (cond
            [(char-upper-case? current-char)
             (cons (char-downcase current-char) (any->camel-case* rest-chars))]
            [(char-alphabetic? current-char) (cons current-char (any->camel-case* rest-chars))]
            [else (loop (cadr char-lst) (cdr rest-chars))]))))))

(define (any->snake/kebab-case* char-lst to-replace replacement [prev-char #f])
  (if (empty? char-lst)
    '()
    (let ([func (λ (pc) (any->snake/kebab-case* (cdr char-lst) to-replace replacement pc))])
      (cond
        [(or (char=? (car char-lst) to-replace)
             (char=? (car char-lst) #\.))
         (cons replacement (func replacement))]
        [(char-upper-case? (car char-lst))
         (define new-char (char-downcase (car char-lst)))
         (if (or (false? prev-char)
                 (char=? prev-char replacement))
           (cons new-char (func #\'))
           (cons replacement (cons new-char (func replacement))))]
        [else (cons (car char-lst) (func (car char-lst)))]))))

(define/contract (any->snake-case key)
  (-> symbol? symbol?)
  (any->snake/kebab-case (symbol->string key) #\- #\_))

(define/contract (any->kebab-case key)
  (-> symbol? symbol?)
  (any->snake/kebab-case (symbol->string key) #\_ #\-))

(define (any->snake/kebab-case key to-replace replacement)
  (string->symbol (list->string (any->snake/kebab-case* (string->list key) to-replace replacement))))

(define (prefix-dot->underscore key)
  (string->symbol (string-replace (symbol->string key) "." "_" #:all? #f)))

(module+ test
  (require rackunit)
  (test-case
    "Conversion to camel case"
    (check-equal? (any->camel-case '_snake_case)
                  'snakeCase)
    (check-equal? (any->camel-case 'snake_case)
                  'snakeCase)
    (check-equal? (any->camel-case 'case_)
                  'case)
    (check-equal? (any->camel-case 'camelCase)
                  'camelCase)
    (check-equal? (any->camel-case 'PascalCase)
                  'pascalCase)
    (check-equal? (any->camel-case '_PascalCase)
                  'pascalCase)
    (check-equal? (any->camel-case '-kebab-case)
                  'kebabCase)
    (check-equal? (any->camel-case 'kebab.case)
                  'kebabCase)
    (check-equal? (any->camel-case 'kebab-case)
                  'kebabCase))
  (test-case
    "Conversion to snake case"
    (check-equal? (any->snake-case 'camelCase)
                  'camel_case)
    (check-equal? (any->snake-case 'PascalCase)
                  'pascal_case)
    (check-equal? (any->snake-case '__--PascalCase)
                  '____pascal_case)
    (check-equal? (any->snake-case 'snake___case)
                  'snake___case)
    (check-equal? (any->snake-case 'snake---case)
                  'snake___case)
    (check-equal? (any->snake-case 'snake-Case)
                  'snake_case)
    (check-equal? (any->snake-case '-Snake-Case)
                  '_snake_case)
    (check-equal? (any->snake-case 'kebab-case-kebab-case)
                  'kebab_case_kebab_case)
    (check-equal? (any->snake-case 'kebab.case.kebab.case)
                  'kebab_case_kebab_case)
    (check-equal? (any->snake-case 'kebab-case-)
                  'kebab_case_))
  (test-case
    "Conversion to kebab case"
    (check-equal? (any->kebab-case 'camelCase)
                  'camel-case)
    (check-equal? (any->kebab-case 'snake_case)
                  'snake-case)
    (check-equal? (any->kebab-case 'PascalCase)
                  'pascal-case)
    (check-equal? (any->kebab-case '__--PascalCase)
                  '----pascal-case)
    (check-equal? (any->kebab-case 'snake---case)
                  'snake---case)
    (check-equal? (any->kebab-case 'snake___case)
                  'snake---case)
    (check-equal? (any->kebab-case 'snake_Case)
                  'snake-case)
    (check-equal? (any->kebab-case '_Snake_Case)
                  '-snake-case)
    (check-equal? (any->kebab-case 'kebab_case_)
                  'kebab-case-)
    (check-equal? (any->kebab-case 'some.case_word)
                  'some-case-word)
    (check-equal? (any->kebab-case 'some.caseWord-test)
                  'some-case-word-test)
    )
  (test-case
    "Prefix dot to underscore"
    (check-equal? (prefix-dot->underscore 'prefix.name)
                  'prefix_name)
    (check-equal? (prefix-dot->underscore 'prefix.name.othername)
                  'prefix_name.othername)
    (check-equal? (prefix-dot->underscore 'name)
                  'name)
    )
)
