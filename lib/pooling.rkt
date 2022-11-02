#lang at-exp racket

(require db
         racket/sandbox)

(provide create-db-connection-pool
         (struct-out db-connection-pool)
         get-connection
         return-connection
         close-connection
         db-connection?
         db-connection-raw-connection
         clear-pool
         exn:pool-limit-reached?)


(define LOCK-ACQUIRING-TIME-LIMIT 2)
; TODO "with" type macros to handle counters/lease status when allocating/leasing/returning/disconnecting
; TODO cleanup counters
(struct db-connection-pool (sema prefix connections-acquired connections-released connections max-connections connection-thunk allocated-connections-number retries) #:transparent #:mutable
  #:methods gen:custom-write
  [(define (write-proc pool output-port output-mode)
     (define port (open-output-string))
     (for ([conn (db-connection-pool-connections pool)])
       (displayln conn port))
     (fprintf output-port (format (~a "(db-connection-pool name: ~a acquired: ~a, released: ~a, max-connections: ~a, connection-thunk: ~a, allocated-connections: ~a\n***Connections***\n" (get-output-string port))
                                  (db-connection-pool-prefix pool) (db-connection-pool-connections-acquired pool) (db-connection-pool-connections-released pool)
                                  (db-connection-pool-max-connections pool) (db-connection-pool-connection-thunk pool) (db-connection-pool-allocated-connections-number pool))))])

(struct db-connection (raw-connection leased? lease-counter) #:transparent #:mutable
  #:methods gen:custom-write
  [(define (write-proc db-conn-struct output-port output-mode)
     (fprintf output-port (format "(connection connected? ~a, leased? ~a, lease-counter: ~a)" (and (connection? (db-connection-raw-connection db-conn-struct))
                                                                                                        (connected? (db-connection-raw-connection db-conn-struct)))
                                  (db-connection-leased? db-conn-struct) (db-connection-lease-counter db-conn-struct))))])

(struct exn:pool-limit-reached exn:fail ())
(struct exn:connection-timed-out exn:fail ())

(struct retries (pool-limit pool-limit-sleep-seconds conn-error conn-error-sleep-seconds conn-timeout))

(define (make-db-connection raw-connection #:lease? lease?)
  (db-connection raw-connection lease? (if lease? 1 0)))


(define (create-db-connection-pool connection-thunk #:max-connections [max-connections +inf.0]
                                #:prefix [prefix "no-prefix"]
                                #:pool-limit-retries [pool-retries 0]
                                #:conn-error-retries [conn-retries 0]
                                #:sleep-on-pool-limit-retry [sleep-on-pool-limit-retry-seconds 1]
                                #:sleep-on-conn-error-retry [sleep-on-conn-error-retry-seconds 1]
                                #:connection-timeout [conn-timeout 5])
  (define pool (db-connection-pool (make-semaphore 1) prefix 0 0 `() max-connections connection-thunk 0
                   (retries pool-retries sleep-on-pool-limit-retry-seconds conn-retries sleep-on-conn-error-retry-seconds conn-timeout)))
  (with-retries pool (with-pool-semaphore-timeout pool (create-new-connection pool #:lease? #f)))
  (log-info "[~a] Pool initialized!" prefix)
  pool)

(define-syntax-rule (with-retries pool body ...)
  (let loop ([pool-retries (retries-pool-limit (db-connection-pool-retries pool))]
             [conn-retries (retries-conn-error (db-connection-pool-retries pool))])
    (with-handlers ([exn:pool-limit-reached?
                      (λ (e)
                         (log-error "[~a] ~a" (db-connection-pool-prefix pool) (exn-message e))
                         (cond
                           [(> pool-retries 0)
                            (log-debug "[~a] Retrying connection (~a retries remaining)" (db-connection-pool-prefix pool) pool-retries)
                            (sleep (retries-pool-limit-sleep-seconds (db-connection-pool-retries pool)))
                            (loop (sub1 pool-retries) conn-retries)]
                           [else (raise e)]))]
                    [exn:connection-timed-out? (λ (e)
                                  (log-error "[~a] ~a" (db-connection-pool-prefix pool) (exn-message e))
                                  (cond
                                    [(> conn-retries 0)
                                     (log-debug "[~a] Retrying connection (~a retries remaining)" (db-connection-pool-prefix pool) conn-retries)
                                     (sleep (retries-conn-error-sleep-seconds (db-connection-pool-retries pool)))
                                     (loop pool-retries (sub1 conn-retries))]
                                    [else (raise e)]))]
                    [exn:fail? (λ (e)
                                  (log-error "[~a] ~a" (db-connection-pool-prefix pool) (exn-message e))
                                  (cond
                                    [(> conn-retries 0)
                                     (log-debug "[~a] Retrying connection (~a retries remaining)" (db-connection-pool-prefix pool) conn-retries)
                                     (sleep (retries-conn-error-sleep-seconds (db-connection-pool-retries pool)))
                                     (loop pool-retries (sub1 conn-retries))]
                                    [else (raise e)]))])
                   body ...)))

(define (get-unused-conn/f conn-lst prefix)
  (if (empty? conn-lst)
    #f
    (let ([conn (car conn-lst)])
      (cond
        [(not (db-connection-leased? conn))
         #| (log-debug "[~a] Unused conn found in pool" prefix) |#
         (set-db-connection-leased?! conn #t)
         (set-db-connection-lease-counter! conn (add1 (db-connection-lease-counter conn)))
         conn]
        [else (get-unused-conn/f (cdr conn-lst) prefix)]))))

(define-syntax-rule (with-pool-semaphore-timeout pool body ...)
  (begin
    (call-with-limits
      LOCK-ACQUIRING-TIME-LIMIT #f
      (thunk (semaphore-wait/enable-break (db-connection-pool-sema pool))))
    (with-handlers ([(λ (_) #t) (λ (e) (semaphore-post (db-connection-pool-sema pool)) (raise e))])
      (define ret ((thunk body ...)))
      (semaphore-post (db-connection-pool-sema pool))
      ret)))

(define (get-connection pool)
  (with-retries pool
    (with-pool-semaphore-timeout pool
      (define maybe-conn
        (get-unused-conn/f (db-connection-pool-connections pool) (db-connection-pool-prefix pool)))
      (or maybe-conn
          (create-new-connection pool #:lease? #t)))))

(define (create-new-connection pool #:lease? lease?)
  (with-handlers ([exn:fail:resource? (λ (e) (raise (exn:connection-timed-out "Connection to db timed out" (current-continuation-marks))))])
    (call-with-limits
      (retries-conn-timeout (db-connection-pool-retries pool)) #f
      (thunk
        (if (< (db-connection-pool-allocated-connections-number pool)
               (db-connection-pool-max-connections pool))
          (let ([raw-conn ((db-connection-pool-connection-thunk pool))])
            (define conn-struct (make-db-connection raw-conn #:lease? lease?))
            (set-db-connection-pool-connections! pool (cons conn-struct (db-connection-pool-connections pool)))
            (set-db-connection-pool-connections-acquired! pool (add1 (db-connection-pool-connections-acquired pool)))
            (set-db-connection-pool-allocated-connections-number! pool (add1 (db-connection-pool-allocated-connections-number pool)))
            (if lease? conn-struct (void)))
          (raise (exn:pool-limit-reached "Connection pool limit reached" (current-continuation-marks))))))))

(define (find-conn conn conn-lst)
  (if (empty? conn-lst)
    (begin
      (log-error (format "Connection not found in pool; connection: ~a; conn-list: ~a" conn conn-lst))
      #f)
    (if (equal? conn (car conn-lst))
      (car conn-lst)
      (find-conn conn (cdr conn-lst)))))

(define/contract (return-connection conn pool)
  (-> db-connection? db-connection-pool? void?)
  (with-pool-semaphore-timeout pool
    (define maybe-conn-struct (find-conn conn (db-connection-pool-connections pool)))
    (define orig-connected? (connected? (db-connection-raw-connection conn)))
    (cond
      [(and (not maybe-conn-struct) orig-connected?)
       (disconnect (db-connection-raw-connection conn))]
      [(and maybe-conn-struct (not orig-connected?))
       (remove-from-pool conn pool)]
      [else
        (set-db-connection-leased?! maybe-conn-struct #f)])))

(define (remove-from-pool conn-struct pool)
  (set-db-connection-pool-connections! pool (remove conn-struct (db-connection-pool-connections pool)))
  (set-db-connection-pool-connections-released! pool (add1 (db-connection-pool-connections-released pool)))
  (set-db-connection-pool-allocated-connections-number! pool (sub1 (db-connection-pool-allocated-connections-number pool))))

(define (close-connection conn pool)
  (log-info "Closing conn ~a, pool: ~a" conn pool)
  (with-pool-semaphore-timeout pool
    (define conn-struct (find-conn conn (db-connection-pool-connections pool)))
    (when conn-struct
      (disconnect (db-connection-raw-connection conn-struct))
      (remove-from-pool conn-struct pool))))

(define (clear-pool pool)
  (map (λ (conn) (close-connection conn pool)) (db-connection-pool-connections pool)))
