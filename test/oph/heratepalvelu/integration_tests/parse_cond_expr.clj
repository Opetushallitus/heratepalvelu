(ns oph.heratepalvelu.integration-tests.parse-cond-expr
  (:require [clojure.string :as s]))

(def binary-operators {"=" = "<>" not= "<" < "<=" <= ">" > ">=" >=})

(def unary-operators
  {"attribute_not_exists" (fn [field] (fn [item] (not (get item field))))})

(defn parse-op [tokens]
  ;; TODO paren case first
  (let [unop (get unary-operators (first tokens))
        unop-rest (rest tokens)]
    (if unop
      ;;TODO verify that parens are in the right places
      (if (and (= (first unop-rest) "(")
               (= (second (rest unop-rest)) ")"))
        [(unop (keyword (second unop-rest))) (rest (rest (rest unop-rest)))]
        (throw (ex-info "Syntaksivirhe" {}))) ;; TODO parempi virhe

      (let [field (keyword (first tokens))
            binop-name (first (rest tokens))
            binop (get binary-operators binop-name)
            r-o-r (rest (rest tokens))]
        (if binop
          [(fn [item]
             (let [left (get item field)
                   right (first r-o-r)]
               (when-not (= (first left) (first right))
                 (throw (ex-info "XYZ" {}))) ;; TODO error message
               (binop (compare (second left) (second right)) 0)))
           (rest r-o-r)]
          (throw (ex-info
                  (str "Operator " binop-name
                       " ei löytynyt. Lisää se tänne, jos käytit sitä.")
                  {})))))))

(defn parse-not [tokens]
  (if (= (first tokens) "NOT")
    (let [[f r] (parse-op (rest tokens))]
      [(fn [item] (not (f item))) r])
    (parse-op tokens)))

(defn parse-and [tokens]
  (let [[lhs-func lhs-rest] (parse-not tokens)]
    (if (= (first lhs-rest) "AND")
      (let [[rhs-func rhs-rest] (parse-not (rest lhs-rest))]
        [(fn [item] (and (lhs-func item) (rhs-func item))) rhs-rest])
      [lhs-func lhs-rest])))

(defn parse-or [tokens]
  (let [[lhs-func lhs-rest] (parse-and tokens)]
    (if (= (first lhs-rest) "OR")
      (let [[rhs-func rhs-rest] (parse-and (rest lhs-rest))]
        [(fn [item] (or (lhs-func item) (rhs-func item))) rhs-rest])
      [lhs-func lhs-rest])))

(defn tokenize [cond-expr]
  (s/split (s/replace (s/replace cond-expr ")" " ) ") "(" " ( ") #" +"))

(defn do-replacements [tokens replacements]
  (let [token (first tokens)]
    (if (not token)
      tokens
      (cons (get replacements token token)
            (do-replacements (rest tokens) replacements)))))

(defn parse [cond-expr expr-attr-names expr-attr-vals]
  (if (or (not cond-expr) (= cond-expr ""))
    (fn [item] true)
    (first (parse-or (do-replacements
                       (tokenize cond-expr)
                       (merge expr-attr-names expr-attr-vals))))))
