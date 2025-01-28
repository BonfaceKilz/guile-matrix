(use-modules (dbi dbi)
	     (rnrs bytevectors)
	     (system foreign)
	     (ice-9 match)
	     (srfi srfi-1)
	     (srfi srfi-26)
	     ((lmdb lmdb) #:prefix mdb:))

(define *connection-settings*
  '((sql-username . "webqtlout")
    (sql-password . "webqtlout")
    (sql-database . "db_webqtl")
    (sql-host . "localhost")
    (sql-port . 3306)))

(define (call-with-database backend connection-string proc)
  (let ((db #f))
    (dynamic-wind (lambda ()
		    (set! db (dbi-open backend connection-string)))
		  (cut proc db)
		  (cut dbi-close db))))

(define (call-with-target-database connection-settings proc)
  (call-with-database "mysql" (string-join
			       (list (assq-ref connection-settings 'sql-username)
				     (assq-ref connection-settings 'sql-password)
				     (assq-ref connection-settings 'sql-database)
				     "tcp"
				     (assq-ref connection-settings 'sql-host)
				     (number->string
				      (assq-ref connection-settings 'sql-port)))
			       ":")
		      proc))

(define (round-off z n)
  (let ((power (expt 10 n)))
    (/ (round (* power z)) power)))

(define (lmdb-save-floats path key values)
  "Save a list of floating point VALUES with KEY into lmdb PATH"
  (mdb:with-env-and-txn
   (path) (env txn)
   (let ((dbi (mdb:dbi-open txn #f 0))
	 (vector-size (make-c-struct (list int) (list (length values))))
	 (s (make-c-struct (make-list (length values) float)
			   values)))
     (mdb:put txn dbi key
	      (mdb:make-val s (apply + (make-list (length values) (sizeof float)))))
     (mdb:put txn dbi "size"
	      (mdb:make-val vector-size
			    (sizeof int))))))

(define (lmdb-get-floats path key)
  "Get a list of floating point values given PATH and a KEY"
  (mdb:with-env-and-txn
   (path) (env txn)
   (let* ((dbi (mdb:dbi-open txn #f 0))
	  (data (mdb:get txn dbi key))
	  (size (car (mdb:val-data-parse (mdb:get txn dbi "size")
					 (list int)))))
     (map (lambda (n) (round-off n 6))
	  (mdb:val-data-parse data (make-list size float))))))

(define (sql-exec db statement)
  (dbi-query db statement)
  (database-check-status db))

(define (sql-fold proc init db statement)
  (sql-exec db statement)
  (let loop ((result init))
    (let ((row (dbi-get_row db)))
      (if row
          (loop (proc row result))
          result))))

(define (sql-for-each proc db statement)
  (sql-fold (lambda (row _)
              (proc row))
            #f db statement))

(define (sql-map proc db statement)
  (sql-fold (lambda (row result)
              (cons (proc row) result))
            (list) db statement))

(define (database-check-status db)
  (match (dbi-get_status db)
    ((code . str)
     (unless (zero? code)
       (error str)))))

;; Example of storing values.
(call-with-target-database
 *connection-settings*
 (lambda (db)
   (let* ((data (sql-map (lambda (x) x)
			 db "SELECT
    Strain.Name as name,
    PublishData.value as value
FROM
    PublishData
    INNER JOIN Strain ON PublishData.StrainId = Strain.Id
    INNER JOIN PublishXRef ON PublishData.Id = PublishXRef.DataId
    INNER JOIN PublishFreeze ON PublishXRef.InbredSetId = PublishFreeze.InbredSetId
LEFT JOIN PublishSE ON
    PublishSE.DataId = PublishData.Id AND
    PublishSE.StrainId = PublishData.StrainId
LEFT JOIN NStrain ON
    NStrain.DataId = PublishData.Id AND
    NStrain.StrainId = PublishData.StrainId
WHERE
	PublishXRef.Id = 10007 AND
	PublishFreeze.Id = 1 AND
    PublishFreeze.public > 0 AND
    PublishFreeze.confidentiality < 1
ORDER BY
    LENGTH(Strain.Name), Strain.Name"))
	  (strains (map (lambda (x) (assoc-ref x "name")) data))
	  (values (map (lambda (x) (assoc-ref x "value")) data)))
     (lmdb-save-floats "/tmp/test" "10007" (reverse values)))))

