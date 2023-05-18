(ns fr33m0nk.utils
  (:import
    (akka.japi Pair)
    (clojure.lang IFn)
    (java.io Serializable)
    (scala Product)))

(deftype Function0
  [f]
  java.util.function.Supplier
  (get [_]
    (f))
  akka.japi.function.Creator
  (create [_]
    (f))
  IFn
  (invoke [_]
    (f))
  Serializable)

(deftype Function1
  [f]
  java.util.function.Consumer
  (accept [_ a]
    (f a))
  IFn
  (invoke [_ a]
    (f a))
  java.util.function.Function
  akka.japi.function.Function
  (apply [_ a]
    (f a))
  java.util.function.Predicate
  akka.japi.function.Predicate
  (test [_ a]
    (f a))
  Serializable)

(deftype Function2
  [f]
  java.util.function.BiConsumer
  (accept [_ a b]
    (f a b))
  IFn
  (invoke [_ a b]
    (f a b))
  java.util.function.BiFunction
  akka.japi.function.Function2
  (apply [_ a b]
    (f a b))
  java.util.function.BiPredicate
  (test [_ a b]
    (f a b))
  Serializable)

(defn ->fn0
  "Wraps a Clojure function with arity 0 in a Scala `Function0`."
  ^Function0
  [f]
  (->Function0 f))

(defn ->fn1
  "Wraps a Clojure function with arity 1 in a Scala `Function1`."
  ^Function1
  [f]
  (->Function1 f))

(defn ->fn2
  "Wraps a Clojure function with arity 2 in a Scala `Function2`."
  ^Function2
  [f]
  (->Function2 f))

(defn pair->vector
  [^Pair pair]
  (let [scala-tuple (.toScala pair)]
    (as-> scala-tuple $
          (.productArity $)
          (take $ (iterate inc 0))
          (into []
                (map #(.productElement ^Product scala-tuple %))
                $))))
