(ns oph.heratepalvelu.integration-tests.mock-db
  (:require [clojure.string :as s]
            [environ.core :refer [env]]
            [oph.heratepalvelu.integration-tests.parse-cond-expr :as pce]))

;; map of lists (each list represents a table)

;; In each list (table), each row is a map as well. Or maybe use sets instead of lists?
;; In any case, we need to make sure that lookup always returns the most recent saved
;; value. With lists, that's easy, since you just cons it on top and then start
;; your search from the beginning of the list. These tests won't generate enough
;; data for memory to be an issue, even if we never garbage collect the mock DB.

;; But we need to be able to return everything that matches a condition without
;; bringing up old values, so maps (keys to values) it is.

(def mock-db-tables (atom {}))

(def mock-db-table-key-fields (atom {}))

;; TODO version with index
(defn- get-table-key-fields [table-name]
  (get @mock-db-table-key-fields table-name))

(defn- get-item-key [table-key-fields item]
  (let [primary-key-field (:primary-key table-key-fields)
        primary-key-value (get item primary-key-field)
        sort-key-field (:sort-key table-key-fields)
        item-key {primary-key-field primary-key-value}]
    (if sort-key-field
      (assoc item-key sort-key-field (get item sort-key-field))
      item-key)))

(defn clear-mock-db []
  (reset! mock-db-tables {})
  (reset! mock-db-table-key-fields {}))

(defn create-table [table-name table-key-fields]
  (reset! mock-db-table-key-fields
          (assoc @mock-db-table-key-fields table-name table-key-fields))
  (reset! mock-db-tables (assoc @mock-db-tables table-name {})))

(defn set-table-contents [table-name items]
  (let [key-fields (get-table-key-fields table-name)]
    (reset! mock-db-tables
            (assoc
              @mock-db-tables
              table-name
              (reduce #(assoc %1 (get-item-key key-fields %2) %2) {} items)))))

(defn get-whole-table [table-name] (get @mock-db-tables table-name))

(defn- strip-attr-vals [item] (into {} (map (fn [[k v]] [k (second v)]) item)))

(defn put-item
  ([item options] (put-item item options (:herate-table env)))
  ([item options table-name]
    (let [item-key (get-item-key (get-table-key-fields table-name) item)
          table (get @mock-db-tables table-name)
          cond-expr-predicate (pce/parse (:cond-expr options)
                                         (:expr-attr-names options)
                                         (:expr-attr-vals options))]
      (when (cond-expr-predicate (get table item-key))
        (reset! mock-db-tables
                (assoc @mock-db-tables
                       table-name
                       (assoc table item-key item)))))))

(defn get-item
  ([key-conds] (get-item key-conds (:herate-table env)))
  ([key-conds table-name]
    (strip-attr-vals (get (get @mock-db-tables table-name) key-conds))))

(defn delete-item
  ([key-conds] (delete-item key-conds (:herate-table env)))
  ([key-conds table-name]
    (let [table (get @mock-db-tables table-name)]
      (reset! mock-db-tables
              (assoc @mock-db-tables table-name (dissoc table key-conds))))))

(defn- attr-val-comparison [op]
  (fn [x y] (if (not= (first x) (first y))
              (throw (ex-info (str (first x) " != " (first y)) {}))
              (op (compare (second x) (second y)) 0))))

(def basic-comparison-operators
  (into {} (map (fn [[k op]] [k (attr-val-comparison op)])
                {:eq = :le <})))

(def comparison-operators
  (merge basic-comparison-operators
         {})) ;;TODO handle i.e. between

(defn- create-key-cond-predicate [key-conds]
  (let [key-conds-seq (seq key-conds)
        [k1 [op1 v1]] (first key-conds-seq)
        [k2 [op2 v2]] (second key-conds-seq)]
    (if k2
      (fn [item] (and ((get comparison-operators op1) (get item k1) v1)
                      ((get comparison-operators op2) (get item k2) v2)))
      (fn [item] ((get comparison-operators op1) (get item k1) v1)))))

(defn- sort-by-index [items index]
  items ;; TODO
  )

(defn query-items
  ([key-conds options] (query-items key-conds options (:herate-table env)))
  ([key-conds options table-name]
    (let [table (get @mock-db-tables table-name)
          index nil ;; TODO
          filter-expr-predicate (pce/parse (:filter-expression options)
                                           (:expr-attr-names options)
                                           (:expr-attr-vals options))
          key-cond-predicate (create-key-cond-predicate key-conds)
          predicate (fn [item] (and (key-cond-predicate item)
                                    (filter-expr-predicate item)))
          items-sorted (sort-by-index (filter predicate (vals table)) index)
          items-limited (if (:limit options)
                          (take (:limit options) items-sorted)
                          items-sorted)]
      (map strip-attr-vals items-limited))))

(defn- parse-update-expr [update-expr attr-names attr-vals]
  (into {} (map (fn [[k v]] [(keyword (get attr-names k k)) (get attr-vals v)])
                (map (fn [x] (s/split x #" *= *"))
                     (s/split (s/replace update-expr #"SET " "") #" *, *")))))

(defn update-item
  ([key-conds options] (update-item key-conds options (:herate-table env)))
  ([key-conds options table-name]
    (let [existing (get (get @mock-db-tables table-name) key-conds)
          updates (parse-update-expr (:update-expr options)
                                     (:expr-attr-names options)
                                     (:expr-attr-vals options))]
      (put-item (merge existing updates)
                {:cond-expr (:cond-expr options)}
                table-name))))
