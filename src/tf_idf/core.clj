(ns tf-idf.core
  (:gen-class)
  (:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de])
  )


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic term handling functions

(def stopwords #{"a" "all" "and" "any" "are" "is" "in" "of" "on"
                 "or" "our" "so" "this" "the" "that" "to" "we"})

(defn terms [content]
  (println "Terms::" content)
  (map string/lower-case (string/split content #" "))
  )

(def remove-stopwords
  (partial remove (partial contains? stopwords)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; tf / idf / tf*idf functions
(defn idf [doc-count doc-count-for-term]
  (Math/log (/ doc-count (+ 1.0 doc-count-for-term))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic Spark management
(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "spark://master:7077")
              ;(conf/master "local[*]")
              (conf/app-name "tfidf")
              (conf/set "spark.driver.port" "7001")
              (conf/set "spark.fileserver.port" "7002")
              (conf/set "spark.broadcast.port" "7003")
              (conf/set "spark.replClassServer.port" "7004")
              (conf/set "spark.blockManager.port" "7005")
              (conf/set "spark.executor.port" "7006")
              (conf/set "spark.broadcast.factory" "org.apache.spark.broadcast.HttpBroadcastFactory")
              )]
    (spark/spark-context c)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Basic data model generation functions
(defn term-count-from-doc
  "Returns a stopword filtered seq of tuples of doc-id,[term term-count doc-terms-count]"
  [doc-id content]
  (let [
        ;terms (remove-stopwords (terms content))
        terms (map string/lower-case (string/split content #" "))
        doc-terms-count (count terms)
        term-count (frequencies terms)]
    (map (fn [term] (spark/tuple [doc-id term] [(term-count term) doc-terms-count]))
         (distinct terms))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Spark Transformations / Actions

(defn document-count [documents]
  (spark/count documents))

(defn term-count-by-doc-term [documents]
  (->>
    documents
    (spark/flat-map-to-pair
      (s-de/key-value-fn term-count-from-doc))
    spark/cache))

(defn document-count-by-term [document-term-count]
  (->> document-term-count
       (spark/map-to-pair (s-de/key-value-fn
                            (fn [[_ term] [_ _]] (spark/tuple term 1))))
       (spark/reduce-by-key +)))

(defn idf-by-term [doc-count doc-count-for-term-rdd]
  (spark/map-values (partial idf doc-count) doc-count-for-term-rdd))

(defn tf-by-doc-term [document-term-count]
  (spark/map-to-pair (s-de/key-value-fn
                       (fn [[doc term] [term-count doc-terms-count]]
                         (spark/tuple term [doc (/ term-count doc-terms-count)])))
                     document-term-count))


(defn tf-idf-by-doc-term [doc-count document-term-count term-idf]
  (->> (spark/join (tf-by-doc-term document-term-count) term-idf)
       (spark/map-to-pair (s-de/key-val-val-fn
                            (fn [term [doc tf] idf]
                              (spark/tuple [doc term] (* tf idf)))))
       ))


(defn tf-idf [corpus]
  (let [doc-count (document-count corpus)
        document-term-count (term-count-by-doc-term corpus)
        term-idf (idf-by-term doc-count (document-count-by-term document-term-count))]
    (tf-idf-by-doc-term doc-count document-term-count term-idf)))

(def tuple-swap (memfn ^scala.Tuple2 swap))

(def swap-key-value (partial spark/map-to-pair tuple-swap))

(defn sort-by-value [rdd]
  (->> rdd
       swap-key-value
       (spark/sort-by-key compare false)
       swap-key-value
       ))

(defn lazy-file-lines [file]
  (letfn [(helper [rdr]
                  (lazy-seq
                    (if-let [line (.readLine rdr)]
                      (cons line (helper rdr))
                      (do (.close rdr) nil))))]
    (helper (clojure.java.io/reader file))))

(defn -main [& args]
  (let [sc (make-spark-context)
        path-to-sentences "/data/deu_news_2010_30K-sentences.txt"
        documents  (map (fn [line]
                          (let [idx (.substring line 0 (.indexOf line "\t"))
                                sentence (.substring line (.indexOf line "\t") (.length line))]
                            (spark/tuple (str "doc-" idx) sentence)))
                        (lazy-file-lines path-to-sentences))
        corpus (spark/parallelize-pairs sc documents)
        tf-idf (tf-idf corpus)
        ]
    (clojure.pprint/pprint (spark/collect tf-idf))))
