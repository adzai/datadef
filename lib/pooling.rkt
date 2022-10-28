#lang at-exp racket

(require db)

(provide create-db-connection-pool
         (struct-out db-connection-pool)
         get-connection
         return-connection
         close-connection
         db-connection?
         db-connection-raw-connection
         exn:pool-limit-reached?)

; TODO "with" type macros to handle counters/lease status when allocating/leasing/returning/disconnecting
; TODO cleanup counters
(struct db-connection-pool (connections-acquired connections-released connections max-connections connection-thunk allocated-connections-number retries sleep-on-retry-seconds) #:transparent #:mutable
  #:methods gen:custom-write
  [(define (write-proc pool output-port output-mode)
     (define port (open-output-string))
     (for ([conn (db-connection-pool-connections pool)])
       (displayln conn port))
     (fprintf output-port (format (~a "(db-connection-pool acquired: ~a, released: ~a, max-connections: ~a, connection-thunk: ~a, allocated-connections: ~a\n***Connections***\n" (get-output-string port))
                                  (db-connection-pool-connections-acquired pool) (db-connection-pool-connections-released pool)
                                  (db-connection-pool-max-connections pool) (db-connection-pool-connection-thunk pool) (db-connection-pool-allocated-connections-number pool))))])

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

(define (create-db-connection-pool connection-thunk #:max-connections [max-connections +inf.0]
                                #:retries [retries 0]
                                #:sleep-on-retry [sleep-on-retry-seconds 1])
  (define db-conn (db-connection (connection-thunk) #f 0))
  (db-connection-pool 1 0 `(,db-conn) max-connections connection-thunk 1
                   retries sleep-on-retry-seconds))

(define (get-unused-conn/f conn-lst prefix)
  (if
    (empty? conn-lst) #f
    (let ([conn (car conn-lst)])
      (cond
        [(not (db-connection-leased? conn))
         (log-debug "[~a] Unused conn found in pool" prefix)
         (set-db-connection-leased?! conn #t)
         (set-db-connection-lease-counter! conn (add1 (db-connection-lease-counter conn)))
         conn]
        [else (get-unused-conn/f (cdr conn-lst) prefix)]))))

(define (get-connection pool
                        #:prefix [prefix "no-prefix"]
                        #:retries [retries (db-connection-pool-retries pool)]
                        #:sleep-on-retry [sleep-on-retry-seconds (db-connection-pool-sleep-on-retry-seconds pool)])
  ; TODO differentiate between network error / pool limit reached for sleep
  ; TODO Timeout on sema?
  (let loop ([retries retries])
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
      (call-with-semaphore/enable-break
        pool-sema
        (thunk
          (define maybe-conn
            (get-unused-conn/f (db-connection-pool-connections pool) prefix))
          (or maybe-conn
              (create-new-connection pool)))))))

(define (create-new-connection pool)
  (if (< (db-connection-pool-allocated-connections-number pool)
         (db-connection-pool-max-connections pool))
    (let ([raw-conn ((db-connection-pool-connection-thunk pool))])
      (define conn-struct (make-db-connection-for-lease raw-conn))
      (set-db-connection-pool-connections! pool (cons conn-struct (db-connection-pool-connections pool)))
      (set-db-connection-pool-connections-acquired! pool (add1 (db-connection-pool-connections-acquired pool)))
      (set-db-connection-pool-allocated-connections-number! pool (add1 (db-connection-pool-allocated-connections-number pool)))
      conn-struct)
    (raise (exn:pool-limit-reached "Connection pool limit reached" (current-continuation-marks)))))

(define (find-conn conn conn-lst)
  (if (empty? conn-lst)
    (error "Connection not found in pool")
    (if (equal? conn (car conn-lst))
      (car conn-lst)
      (find-conn conn (cdr conn-lst)))))

(define (return-connection conn pool)
  (call-with-semaphore
    pool-sema
    (thunk (set-db-connection-leased?! (find-conn conn (db-connection-pool-connections pool)) #f))))

(define (close-connection conn pool)
  (call-with-semaphore
    pool-sema
    (thunk
      (define conn-struct (find-conn conn (db-connection-pool-connections pool)))
      (disconnect (db-connection-raw-connection conn-struct))
      (set-db-connection-pool-connections! pool (remove conn-struct (db-connection-pool-connections pool)))
      (set-db-connection-pool-connections-released! pool (add1 (db-connection-pool-connections-released pool)))
      (set-db-connection-pool-allocated-connections-number! pool (sub1 (db-connection-pool-allocated-connections-number pool))))))
