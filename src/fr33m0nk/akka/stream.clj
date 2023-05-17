(ns fr33m0nk.akka.stream
  (:refer-clojure :exclude [map mapcat filter reduce drop take concat drop-while take-while group-by merge])
  (:require [fr33m0nk.utils :as utils])
  (:import (akka.actor ActorSystem)
           (akka.stream Graph Materializer SubstreamCancelStrategy)
           (akka.stream.javadsl Flow RunnableGraph Source SubFlow SubSource)
           (java.util List)
           (java.util.concurrent CompletableFuture)))

(set! *warn-on-reflection* true)

(def draining-substream-cancel-strategy (SubstreamCancelStrategy/drain))

(def propagate-substream-cancel-strategy (SubstreamCancelStrategy/propagate))

(defprotocol IStreamOperations
  (map [this mapping-fn])
  (mapcat [this mapping-fn])
  (map-async [this parallelism mapping-fn] [this parallelism mapping-fn then-fn])
  (map-async-unordered [this parallelism mapping-fn] [this parallelism mapping-fn then-fn])
  (map-materialized-value [this mapping-fn])
  (filter [this pred?])
  (filter-falsy [this pred?])
  (fold [this zero-value fold-fn])
  (fold-async [this zero-value fold-fn] [this zero-value fold-fn then-fn])
  (concat [this ^Graph stream])
  (concat-lazy [this ^Graph stream])
  (concat-all [this streams])
  (reduce [this reducer-fn])
  (via [this ^Graph flow])
  (via-mat [this ^Graph flow combiner-fn])
  (to [this ^Graph sink])
  (to-mat [this ^Graph sink combiner-fn])
  (run [this ^ActorSystem actor-system])
  (run-with [this ^Graph source ^Graph sink materializer-or-actor-system])
  (drop [this n])
  (drop-while [this pred?])
  (take [this n])
  (take-while [this pred?] [this pred? inclusive?])
  (sliding [this count step-size])
  (grouped [this size])
  (group-by [this max-substreams grouping-fn] [this max-substreams grouping-fn allow-closed-substream-recreation?])
  (split-when [this pred?] [this ^SubstreamCancelStrategy substream-cancel-strategy pred?])
  (split-after [this pred?] [this ^SubstreamCancelStrategy substream-cancel-strategy pred?])
  (flatmap-concat [this mapping-fn])
  (flatmap-merge [this breadth mapping-fn])
  (merge [this ^Graph stream] [this ^Graph stream eager-complete?])
  (merge-all [this streams eager-complete?]))

(defprotocol ISubStreamOperations
  (merge-substreams [this])
  (merge-substreams-with-parallelism [this parallelism])
  (concat-substreams [this])
  (also-to-all [this streams]))

(extend-type Flow
  IStreamOperations
  (map [this mapping-fn]
    (.map this (utils/->fn1 mapping-fn)))
  (mapcat [this mapping-fn]
    (.mapConcat this (utils/->fn1 mapping-fn)))
  (map-async [this parallelism mapping-fn]
    (.mapAsync this (int parallelism)
               (utils/->fn1 (fn [arg]
                              (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))))))
  (map-async [this parallelism mapping-fn then-fn]
    (.mapAsync this (int parallelism) (utils/->fn1 (fn [arg]
                                                     (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                         (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (map-async-unordered [this parallelism mapping-fn]
    (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                              (CompletableFuture/supplyAsync
                                                                (utils/->fn0 (fn [] (mapping-fn arg))))))))
  (map-async-unordered [this parallelism mapping-fn then-fn]
    (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                              (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                                  (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (map-materialized-value [this mapping-fn]
    (.mapMaterializedValue this (utils/->fn2 mapping-fn)))
  (filter [this pred?]
    (.filter this (utils/->fn1 pred?)))
  (filter-falsy [this pred?]
    (.filterNot this (utils/->fn1 (complement pred?))))
  (fold [this zero-value fold-fn]
    (.fold this zero-value (utils/->fn2 (fn [arg1 arg2] (fold-fn arg1 arg2)))))
  (fold-async [this zero-value fold-fn]
    (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                               (CompletableFuture/supplyAsync
                                                 (utils/->fn0 (fn [] (fold-fn arg1 arg2))))))))
  (fold-async [this zero-value fold-fn then-fn]
    (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                               (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (fold-fn arg1 arg2))))
                                                   (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (concat [this ^Graph stream]
    (.concat this stream))
  (concat-lazy [this ^Graph stream]
    (.concatLazy this stream))
  (concat-all [this streams]
    (.concatAllLazy this ^"[Lakka.stream.Graph;" (into-array Graph streams)))
  (reduce [this reducer-fn]
    (.reduce this (utils/->fn2 (fn [arg1 arg2] (reducer-fn arg1 arg2)))))
  (via [this ^Graph flow]
    (.via this flow))
  (via-mat [this ^Graph flow combiner-fn]
    (.viaMat this flow (utils/->fn2 (fn [arg1 arg2] (combiner-fn arg1 arg2)))))
  (to [this ^Graph sink]
    (.to this sink))
  (to-mat [this ^Graph sink combiner-fn]
    (.toMat this sink (utils/->fn2 (fn [arg1 arg2] (combiner-fn arg1 arg2)))))
  (run-with [this ^Graph source ^Graph sink materializer-or-actor-system]
    (condp instance? materializer-or-actor-system
      ActorSystem (.runWith this source sink ^ActorSystem materializer-or-actor-system)
      Materializer (.runWith this source sink ^Materializer materializer-or-actor-system)))
  (drop [this n]
    (.drop this n))
  (drop-while [this pred?]
    (.dropWhile this (utils/->fn1 pred?)))
  (take [this n]
    (.take this n))
  (take-while [this pred?]
    (.takeWhile this (utils/->fn1 pred?)))
  (take-while [this pred? inclusive?]
    (.takeWhile this (utils/->fn1 pred?) inclusive?))
  (sliding [this count step-size]
    (.sliding this (int count) (int step-size)))
  (grouped [this size]
    (.grouped this (int size)))
  (group-by [this max-substreams grouping-fn]
    (.groupBy this (int max-substreams) (utils/->fn1 grouping-fn)))
  (group-by [this max-substreams grouping-fn allow-closed-substream-recreation?]
    (.groupBy this (int max-substreams) (utils/->fn1 grouping-fn) allow-closed-substream-recreation?))
  (split-when [this pred?]
    (.splitWhen this pred?))
  (split-when [this ^SubstreamCancelStrategy substream-cancel-strategy pred?]
    (.splitAfter this substream-cancel-strategy pred?))
  (split-after [this pred?]
    (.splitAfter this pred?))
  (split-after [this ^SubstreamCancelStrategy substream-cancel-strategy pred?]
    (.splitAfter this substream-cancel-strategy pred?))
  (flatmap-concat [this mapping-fn]
    (.flatMapConcat this (utils/->fn1 mapping-fn)))
  (flatmap-merge [this breadth mapping-fn]
    (.flatMapMerge this (int breadth) (utils/->fn1 mapping-fn)))
  (merge [this ^Graph stream]
    (.merge this stream))
  (merge [this ^Graph stream eager-complete?]
    (.merge this stream eager-complete?))
  (merge-all [this streams eager-complete?]
    (.mergeAll this ^List streams eager-complete?)))

(extend-type SubFlow
  IStreamOperations
  (map [this mapping-fn]
    (.map this (utils/->fn1 mapping-fn)))
  (mapcat [this mapping-fn]
    (.mapConcat this (utils/->fn1 mapping-fn)))
  (map-async [this parallelism mapping-fn]
    (.mapAsync this (int parallelism)
               (utils/->fn1 (fn [arg]
                              (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))))))
  (map-async [this parallelism mapping-fn then-fn]
    (.mapAsync this (int parallelism) (utils/->fn1 (fn [arg]
                                                     (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                         (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (map-async-unordered [this parallelism mapping-fn]
    (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                              (CompletableFuture/supplyAsync
                                                                (utils/->fn0 (fn [] (mapping-fn arg))))))))
  (map-async-unordered [this parallelism mapping-fn then-fn]
    (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                              (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                                  (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (filter [this pred?]
    (.filter this (utils/->fn1 pred?)))
  (filter-falsy [this pred?]
    (.filterNot this (utils/->fn1 (complement pred?))))
  (fold [this zero-value fold-fn]
    (.fold this zero-value (utils/->fn2 (fn [arg1 arg2] (fold-fn arg1 arg2)))))
  (fold-async [this zero-value fold-fn]
    (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                               (CompletableFuture/supplyAsync
                                                 (utils/->fn0 (fn [] (fold-fn arg1 arg2))))))))
  (fold-async [this zero-value fold-fn then-fn]
    (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                               (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (fold-fn arg1 arg2))))
                                                   (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (concat [this ^Graph stream]
    (.concat this stream))
  (concat-lazy [this ^Graph stream]
    (.concatLazy this stream))
  (concat-all [this streams]
    (.concatAllLazy this ^"[Lakka.stream.Graph;" (into-array Graph streams)))
  (reduce [this reducer-fn]
    (.reduce this (utils/->fn2 (fn [arg1 arg2] (reducer-fn arg1 arg2)))))
  (via [this ^Graph flow]
    (.via this flow))
  (to [this ^Graph sink]
    (.to this sink))
  (drop [this n]
    (.drop this n))
  (drop-while [this pred?]
    (.dropWhile this (utils/->fn1 pred?)))
  (grouped [this size]
    (.grouped this (int size)))
  (take [this n]
    (.take this n))
  (take-while [this pred?]
    (.takeWhile this (utils/->fn1 pred?)))
  (take-while [this pred? inclusive?]
    (.takeWhile this (utils/->fn1 pred?) inclusive?))
  (sliding [this count step-size]
    (.sliding this (int count) (int step-size)))
  (flatmap-concat [this mapping-fn]
    (.flatMapConcat this (utils/->fn1 mapping-fn)))
  (flatmap-merge [this breadth mapping-fn]
    (.flatMapMerge this (int breadth) (utils/->fn1 mapping-fn)))
  (merge [this ^Graph stream]
    (.merge this stream))
  (merge-all [this streams eager-complete?]
    (.mergeAll this ^List streams eager-complete?))
  ISubStreamOperations
  (merge-substreams [this]
    (.mergeSubstreams this))
  (merge-substreams-with-parallelism [this parallelism]
    (.mergeSubstreamsWithParallelism this (int parallelism)))
  (concat-substreams [this]
    (.concatSubstreams this))
  (also-to-all [this streams]
    (.alsoToAll this ^"[Lakka.stream.Graph;" (into-array Graph streams))))

(extend-type Source
  IStreamOperations
  (map [this mapping-fn]
    (.map this (utils/->fn1 mapping-fn)))
  (mapcat [this mapping-fn]
    (.mapConcat this (utils/->fn1 mapping-fn)))
  (map-async [this parallelism mapping-fn]
    (.mapAsync this (int parallelism)
               (utils/->fn1 (fn [arg]
                              (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))))))
  (map-async [this parallelism mapping-fn then-fn]
    (.mapAsync this (int parallelism) (utils/->fn1 (fn [arg]
                                                     (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                         (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (map-async-unordered [this parallelism mapping-fn]
    (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                              (CompletableFuture/supplyAsync
                                                                (utils/->fn0 (fn [] (mapping-fn arg))))))))
  (map-async-unordered [this parallelism mapping-fn then-fn]
    (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                              (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                                  (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (map-materialized-value [this mapping-fn]
    (.mapMaterializedValue this (utils/->fn2 mapping-fn)))
  (filter [this pred?]
    (.filter this (utils/->fn1 pred?)))
  (filter-falsy [this pred?]
    (.filterNot this (utils/->fn1 (complement pred?))))
  (fold [this zero-value fold-fn]
    (.fold this zero-value (utils/->fn2 (fn [arg1 arg2] (fold-fn arg1 arg2)))))
  (fold-async [this zero-value fold-fn]
    (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                               (CompletableFuture/supplyAsync
                                                 (utils/->fn0 (fn [] (fold-fn arg1 arg2))))))))
  (fold-async [this zero-value fold-fn then-fn]
    (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                               (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (fold-fn arg1 arg2))))
                                                   (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (concat [this ^Graph stream]
    (.concat this stream))
  (concat-lazy [this ^Graph stream]
    (.concatLazy this stream))
  (concat-all [this streams]
    (.concatAllLazy this ^"[Lakka.stream.Graph;" (into-array Graph streams)))
  (reduce [this reducer-fn]
    (.reduce this (utils/->fn2 (fn [arg1 arg2] (reducer-fn arg1 arg2)))))
  (via [this ^Graph flow]
    (.via this flow))
  (via-mat [this ^Graph flow combiner-fn]
    (.viaMat this flow (utils/->fn2 (fn [arg1 arg2] (combiner-fn arg1 arg2)))))
  (to [this ^Graph sink]
    (.to this sink))
  (to-mat [this ^Graph sink combiner-fn]
    (.toMat this sink (utils/->fn2 (fn [arg1 arg2] (combiner-fn arg1 arg2)))))
  (run [this materializer-or-actor-system]
    (condp instance? materializer-or-actor-system
      ActorSystem (.run this ^ActorSystem materializer-or-actor-system)
      Materializer (.run this ^Materializer materializer-or-actor-system)))
  (run-with [this ^Graph sink materializer-or-actor-system]
    (condp instance? materializer-or-actor-system
      ActorSystem (.runWith this sink ^ActorSystem materializer-or-actor-system)
      Materializer (.runWith this sink ^Materializer materializer-or-actor-system)))
  (drop [this n]
    (.drop this n))
  (drop-while [this pred?]
    (.dropWhile this (utils/->fn1 pred?)))
  (take [this n]
    (.take this n))
  (take-while [this pred?]
    (.takeWhile this (utils/->fn1 pred?)))
  (take-while [this pred? inclusive?]
    (.takeWhile this (utils/->fn1 pred?) inclusive?))
  (sliding [this count step-size]
    (.sliding this (int count) (int step-size)))
  (grouped [this size]
    (.grouped this (int size)))
  (group-by [this max-substreams grouping-fn]
    (.groupBy this (int max-substreams) (utils/->fn1 grouping-fn)))
  (group-by [this max-substreams grouping-fn allow-closed-substream-recreation?]
    (.groupBy this (int max-substreams) (utils/->fn1 grouping-fn) allow-closed-substream-recreation?))
  (split-when [this pred?]
    (.splitWhen this pred?))
  (split-when [this ^SubstreamCancelStrategy substream-cancel-strategy pred?]
    (.splitAfter this substream-cancel-strategy pred?))
  (split-after [this pred?]
    (.splitAfter this pred?))
  (split-after [this ^SubstreamCancelStrategy substream-cancel-strategy pred?]
    (.splitAfter this substream-cancel-strategy pred?))
  (flatmap-concat [this mapping-fn]
    (.flatMapConcat this (utils/->fn1 mapping-fn)))
  (flatmap-merge [this breadth mapping-fn]
    (.flatMapMerge this (int breadth) (utils/->fn1 mapping-fn)))
  (merge [this ^Graph stream]
    (.merge this stream))
  (merge [this ^Graph stream eager-complete?]
    (.merge this stream eager-complete?))
  (merge-all [this streams eager-complete?]
    (.mergeAll this ^List streams eager-complete?)))

(extend-type SubSource
  IStreamOperations
  (map [this mapping-fn]
    (.map this (utils/->fn1 mapping-fn)))
  (mapcat [this mapping-fn]
    (.mapConcat this (utils/->fn1 mapping-fn)))
  (map-async [this parallelism mapping-fn]
    (.mapAsync this (int parallelism)
               (utils/->fn1 (fn [arg]
                              (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))))))
  (map-async [this parallelism mapping-fn then-fn]
    (.mapAsync this (int parallelism) (utils/->fn1 (fn [arg]
                                                     (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                         (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (map-async-unordered [this parallelism mapping-fn]
    (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                              (CompletableFuture/supplyAsync
                                                                (utils/->fn0 (fn [] (mapping-fn arg))))))))
  (map-async-unordered [this parallelism mapping-fn then-fn]
    (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                              (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                                  (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (filter [this pred?]
    (.filter this (utils/->fn1 pred?)))
  (filter-falsy [this pred?]
    (.filterNot this (utils/->fn1 (complement pred?))))
  (fold [this zero-value fold-fn]
    (.fold this zero-value (utils/->fn2 (fn [arg1 arg2] (fold-fn arg1 arg2)))))
  (fold-async [this zero-value fold-fn]
    (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                               (CompletableFuture/supplyAsync
                                                 (utils/->fn0 (fn [] (fold-fn arg1 arg2))))))))
  (fold-async [this zero-value fold-fn then-fn]
    (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                               (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (fold-fn arg1 arg2))))
                                                   (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg)))))))))
  (concat [this ^Graph stream]
    (.concat this stream))
  (concat-lazy [this ^Graph stream]
    (.concatLazy this stream))
  (concat-all [this streams]
    (.concatAllLazy this ^"[Lakka.stream.Graph;" (into-array Graph streams)))
  (reduce [this reducer-fn]
    (.reduce this (utils/->fn2 (fn [arg1 arg2] (reducer-fn arg1 arg2)))))
  (via [this ^Graph flow]
    (.via this flow))
  (to [this ^Graph sink]
    (.to this sink))
  (drop [this n]
    (.drop this n))
  (drop-while [this pred?]
    (.dropWhile this (utils/->fn1 pred?)))
  (take [this n]
    (.take this n))
  (take-while [this pred?]
    (.takeWhile this (utils/->fn1 pred?)))
  (take-while [this pred? inclusive?]
    (.takeWhile this (utils/->fn1 pred?) inclusive?))
  (sliding [this count step-size]
    (.sliding this (int count) (int step-size)))
  (grouped [this size]
    (.grouped this (int size)))
  (flatmap-concat [this mapping-fn]
    (.flatMapConcat this (utils/->fn1 mapping-fn)))
  (flatmap-merge [this breadth mapping-fn]
    (.flatMapMerge this (int breadth) (utils/->fn1 mapping-fn)))
  (merge [this ^Graph stream]
    (.merge this stream))
  (merge-all [this streams eager-complete?]
    (.mergeAll this ^List streams eager-complete?))
  ISubStreamOperations
  (merge-substreams [this]
    (.mergeSubstreams this))
  (merge-substreams-with-parallelism [this parallelism]
    (.mergeSubstreamsWithParallelism this (int parallelism)))
  (concat-substreams [this]
    (.concatSubstreams this))
  (also-to-all [this streams]
    (.alsoToAll this ^"[Lakka.stream.Graph;" (into-array Graph streams))))

(extend-type RunnableGraph
  IStreamOperations
  (map-materialized-value [this mapping-fn]
    (.mapMaterializedValue this (utils/->fn2 mapping-fn)))
  (run [this materializer-or-actor-system]
    (condp instance? materializer-or-actor-system
      ActorSystem (.run this ^ActorSystem materializer-or-actor-system)
      Materializer (.run this ^Materializer materializer-or-actor-system))))

(defn create-flow
  []
  (Flow/create))

(defn flow-from-graph
  [^Graph graph]
  (Flow/fromGraph graph))

(defn flow-from-function
  [f]
  (Flow/fromFunction (utils/->fn1 f)))

(defn flow-from-sink-and-source
  [^Graph sink ^Graph source]
  (Flow/fromSinkAndSource sink source))

(defn flow-from-materializer
  [f]
  (Flow/fromMaterializer (utils/->fn2 f)))

(defn source-from-graph
  [^Graph graph]
  (Source/fromGraph graph))

(defn source-from-materializer
  [f]
  (Source/fromMaterializer (utils/->fn2 f)))
