(use-modules (dbi dbi)
	     (rnrs bytevectors)
	     (system foreign)
	     (ice-9 match)
	     (srfi srfi-1)
	     (srfi srfi-26)
	     (rnrs io ports)
	     (hashing md5)
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

(define* (lmdb-save path key num)
  "Save a NUM with KEY to PATH."
  (mdb:with-env-and-txn
   (path) (env txn)
   (let ((dbi (mdb:dbi-open txn #f 0)))
     (mdb:put txn dbi key
	      (number->string num)))))

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

(define (save-dataset-values)
  (call-with-target-database
   *connection-settings*
   (lambda (db)
     (sql-for-each
      (lambda (row)
	(match row
	  ((("Name" . dataset-name)
	    ("Id" . trait-id))
	   (let* ((md5-hash
		   (md5->string (md5 (string->bytevector (format #f "~a-~a" dataset-name trait-id)
							 (make-transcoder (utf-8-codec))))))
		  (data-dir (format #f "/export5/lmdb-data-hashes/~a" md5-hash))
		  (data-query (format #f "SELECT Strain.Name as name, PublishData.value as value
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
    PublishFreeze.Name = \"~a\" AND
    PublishXRef.Id = ~a AND
    PublishFreeze.public > 0 AND
    PublishData.value IS NOT NULL AND
    PublishFreeze.confidentiality < 1
ORDER BY
    LENGTH(Strain.Name), Strain.Name" dataset-name trait-id)))
	     (format #t "Writing ~a-~a to: ~a" dataset-name trait-id data-dir)
	     (newline)
	     (call-with-target-database
	      *connection-settings*
	      (lambda (db2)
		(sql-for-each
		 (lambda (row)
		   (match row
		     ((("name" . strain-name)
		       ("value" . strain-value))
		      (unless (file-exists? data-dir)
			(mkdir data-dir))
		      (lmdb-save data-dir strain-name strain-value))))
		 db2 data-query)))))))
      db
      "SELECT DISTINCT PublishFreeze.Name, PublishXRef.Id FROM
PublishData INNER JOIN Strain ON PublishData.StrainId = Strain.Id
INNER JOIN PublishXRef ON PublishData.Id = PublishXRef.DataId
INNER JOIN PublishFreeze ON PublishXRef.InbredSetId = PublishFreeze.InbredSetId
LEFT JOIN PublishSE ON
    PublishSE.DataId = PublishData.Id AND
    PublishSE.StrainId = PublishData.StrainId
LEFT JOIN NStrain ON
    NStrain.DataId = PublishData.Id AND
    NStrain.StrainId = PublishData.StrainId
WHERE
    PublishFreeze.public > 0 AND
    PublishFreeze.confidentiality < 1
ORDER BY
    PublishFreeze.Id, PublishXRef.Id"))))

(save-dataset-values)
