#lang at-exp racket

(require "utils.rkt")

(provide set-mock-data!
         db-mocking-data)

(define db-mocking-data (make-parameter #f))

(define (group-mock-data data-list positions)
  (define positions-list (if (list? positions)
                           positions
                           (list positions)))
  (for/list ([pos positions-list])
    (for/list ([data data-list])
      (list-ref data pos))))

 (define (parse-datadef-part dd part)
   (define len (length part))
   (match len
     [0 (db-mock #f (void))]
     [1 (db-mock #f (car part))]
     [2 (db-mock (car part) (cadr part))]
     [_ (error (format "Wrong number of args, part: ~a" part))] ; TODO better err msg
))

(define (parse-db-part data)
  (cond
    [(and (list? data) (= (length data) 1))
     (define dat (if (list? (car (car data))) (car data) (map list (car data))))
     (db-mock dat (void))]
    [(and (list? data) (= (length data) 2))
     (define dat (if (list? (car (car data))) (car data) (map list (car data))))
      (db-mock dat (cadr data))]
    [else (error (format "Wrong number of args, data: ~a" data))])) ; TODO better err msg


(define (set-mock-data! mock-data-list)
  (for ([part mock-data-list])
    (define key (car part))
    (define mock (if (regexp-match? #rx"datadef:" (symbol->string key))
      (parse-datadef-part key (cdr part))
      (parse-db-part (cdr part))))
    (hash-set! (db-mocking-data) key mock)))

