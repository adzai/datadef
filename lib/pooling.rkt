#lang at-exp racket

(require db)

(provide create-connection-pool
         get-connection
         return-connection
         close-connection
         db-connection?
         db-connection-raw-connection)

; TODO "with" type macros to handle counters/lease status when allocating/leasing/returning/disconnecting
; TODO cleanup counters
(struct connection-pool (connections-acquired connections-released connections max-connections connection-thunk allocated-connections-number) #:transparent #:mutable
  #:methods gen:custom-write
  [(define (write-proc pool output-port output-mode)
     (fprintf output-port (format "(connection-pool acquired: ~a, released: ~a, connections: ~a, max-connections: ~a, connection-thunk: ~a, allocated-connections: ~a)"
                                  (connection-pool-connections-acquired pool) (connection-pool-connections-released pool) (connection-pool-connections pool)
                                  (connection-pool-max-connections pool) (connection-pool-connection-thunk pool) (connection-pool-allocated-connections-number pool))))])

(struct db-connection (raw-connection leased? lease-counter) #:transparent #:mutable
  #:methods gen:custom-write
  [(define (write-proc db-conn-struct output-port output-mode)
     (fprintf output-port (format "(connection connected? ~a, leased? ~a, lease-counter: ~a)" (and (connection? (db-connection-raw-connection db-conn-struct))
                                                                                                        (connected? (db-connection-raw-connection db-conn-struct)))
                                  (db-connection-leased? db-conn-struct) (db-connection-lease-counter db-conn-struct))))])

(struct exn:pool-limit-reached exn:fail ())

(define (make-db-connection-for-lease raw-connection)
  (db-connection raw-connection #t 1))

(define pool-sema (make-semaphore 1))

(define (create-connection-pool connection-thunk #:max-connections [max-connections +inf.0])
  (define db-conn (db-connection (connection-thunk) #f 0))
  (connection-pool 1 0 `(,db-conn) max-connections connection-thunk 1))

(define (get-unused-conn/f conn-lst prefix)
  (if
    (empty? conn-lst) #f
    (let ([conn (car conn-lst)])
      (cond
        [(not (db-connection-leased? conn))
         (set-db-connection-leased?! conn #t)
         (set-db-connection-lease-counter! conn (add1 (db-connection-lease-counter conn)))
         conn]
        [else (get-unused-conn/f (cdr conn-lst) prefix)]))))

(define (get-connection pool
                        #:prefix [prefix "no-prefix"]
                        #:retries [retries 0]
                        #:sleep-on-retry [sleep-on-retry-seconds 1])
  ; TODO differentiate between network error / pool limit reached for sleep
  ; TODO Timeout on sema?
  (let loop ([retries retries])
    (call-with-semaphore
      pool-sema
      (thunk
        ; TODO with-handlers
        (with-handlers ([exn:pool-limit-reached?
                          (λ (e)
                             (log-error "[~a] ~a" prefix (exn-message e))
                             (cond
                               [(> retries 0)
                                (log-debug "[~a] Retrying connection..." prefix)
                                (sleep sleep-on-retry-seconds)
                                (loop (sub1 retries))]
                               [else (raise e)]))]
                        [exn:fail? (λ (e)
                                      (log-error "[~a] ~a" prefix (exn-message e))
                                      (cond
                                        [(> retries 0)
                                         (log-debug "[~a] Retrying connection..." prefix)
                                         (sleep sleep-on-retry-seconds)
                                         (loop (sub1 retries))]
                                        [else (raise e)]))])
                       (define maybe-conn (get-unused-conn/f (connection-pool-connections pool) prefix))
                       (or maybe-conn
                         (create-new-connection pool prefix)))))))

(define (create-new-connection pool prefix)
  (if (< (connection-pool-allocated-connections-number pool)
         (connection-pool-max-connections pool))
    (let ([raw-conn ((connection-pool-connection-thunk pool))])
      (define conn-struct (make-db-connection-for-lease raw-conn))
      (set-connection-pool-connections! pool (cons conn-struct (connection-pool-connections pool)))
      (set-connection-pool-connections-acquired! pool (add1 (connection-pool-connections-acquired pool)))
      (set-connection-pool-allocated-connections-number! pool (add1 (connection-pool-allocated-connections-number pool)))
      conn-struct)
    (raise (exn:pool-limit-reached (format "[~a] Connection pool limit reached" prefix) (current-continuation-marks)))))

(define (find-conn conn conn-lst prefix)
  (if (empty? conn-lst)
    (error "Connection not found in pool")
    (if (equal? conn (car conn-lst))
      (car conn-lst)
      (find-conn conn (cdr conn-lst) prefix))))

(define (return-connection conn pool prefix)
  (call-with-semaphore
    pool-sema
    (thunk (set-db-connection-leased?! (find-conn conn (connection-pool-connections pool) prefix) #f))))

(define (close-connection conn pool prefix)
  (define conn-struct (find-conn conn (connection-pool-connections pool) prefix))
  (disconnect (db-connection-raw-connection conn-struct))
  (set-connection-pool-connections! pool (remove conn-struct (connection-pool-connections pool)))
  (set-connection-pool-connections-released! pool (add1 (connection-pool-connections-released pool)))
  (set-connection-pool-allocated-connections-number! pool (sub1 (connection-pool-allocated-connections-number pool))))
