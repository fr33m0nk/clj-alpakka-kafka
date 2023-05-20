(ns fr33m0nk.akka.stream
  (:refer-clojure :exclude [map mapcat filter reduce drop take concat drop-while take-while group-by merge])
  (:require [fr33m0nk.utils :as utils])
  (:import (akka.actor ActorSystem)
           (akka.stream Graph Materializer SubstreamCancelStrategy)
           (akka.stream.javadsl Flow RunnableGraph Sink Source SubFlow SubSource)
           (java.util List)
           (java.util.concurrent CompletableFuture)))

(set! *warn-on-reflection* true)

(def draining-substream-cancel-strategy (SubstreamCancelStrategy/drain))

(def propagate-substream-cancel-strategy (SubstreamCancelStrategy/propagate))

(defprotocol IStreamOperations
  (map [source-or-flow mapping-fn]
    "Transform each element in the stream by calling a mapping function with it and passing the returned value downstream
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/map.html")
  (mapcat [source-or-flow mapping-fn]
    "Transform each element into zero or more elements that are individually passed downstream
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/mapConcat.html")
  (map-async
    [source-or-flow parallelism mapping-fn]
    [source-or-flow parallelism mapping-fn then-fn]
    "Pass incoming elements to a function that return a CompletionStage result
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/mapAsync.html")
  (map-async-unordered
    [source-or-flow parallelism mapping-fn]
    [source-or-flow parallelism mapping-fn then-fn]
    "Like mapAsync but CompletionStage results are passed downstream as they arrive regardless of the order of the elements that triggered them
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/mapAsyncUnordered.html")
  (map-async-partitioned [source-or-flow parallelism per-partition-count partitioner-fn bi-mapper-fn]
    "Pass incoming elements to a function that extracts a partitioning key from the element, then to a function that returns a CompletionStage result, bounding the number of incomplete CompletionStages per partitioning key.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/mapAsyncPartitioned.html")
  (map-materialized-value [source-or-flow mapping-fn])
  (filter [source-or-flow pred?]
    "Filter the incoming elements using a predicate.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/filter.html")
  (filter-falsy [source-or-flow pred?]
    "Filter the incoming elements using a predicate
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/filterNot.html")
  (fold [source-or-flow zero-value fold-fn]
    "Start with current value zero and then apply the current and next value to the given function. When upstream completes, the current value is emitted downstream.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/fold.html")
  (fold-async
    [source-or-flow zero-value fold-fn]
    [source-or-flow zero-value fold-fn then-fn]
    "Just like fold but receives a function that results in a CompletionStage to the next value.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/foldAsync.html")
  (concat [source-or-flow ^Graph stream]
    "After completion of the original upstream the elements of the given source will be emitted.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/concat.html")
  (concat-lazy [source-or-flow ^Graph stream]
    "After completion of the original upstream the elements of the given source will be emitted.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/concatLazy.html")
  (concat-all-lazy [source-or-flow streams]
    "After completion of the original upstream the elements of the given sources will be emitted sequentially
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/concatAllLazy.html")
  (reduce [source-or-flow reducer-fn]
    "Start with first element and then apply the current and next value to the given function, when upstream complete the current value is emitted downstream.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/reduce.html")
  (via [source-or-flow ^Graph flow])
  (via-mat [source-or-flow ^Graph flow combiner-fn])
  (to [source-or-flow ^Graph sink])
  (to-mat [source-or-flow ^Graph sink combiner-fn])
  (run [source-or-flow ^ActorSystem actor-system])
  (run-with
    [source ^Graph sink materializer-or-actor-system]
    [flow ^Graph source ^Graph sink materializer-or-actor-system])
  (drop [source-or-flow n]
    "Drop n elements and then pass any subsequent element downstream.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/drop.html")
  (drop-while [source-or-flow pred?]
    "Drop elements as long as a predicate function return true for the element
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/dropWhile.html")
  (take [source-or-flow n]
    "Pass n incoming elements downstream and then complete
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/take.html")
  (take-while
    [source-or-flow pred?]
    [source-or-flow pred? inclusive?]
    "Pass elements downstream as long as a predicate function returns true and then complete.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/takeWhile.html")
  (sliding [source-or-flow count step-size]
    "Provide a sliding window over the incoming stream and pass the windows as groups of elements downstream.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/sliding.html")
  (grouped [source-or-flow size]
    "Accumulate incoming events until the specified number of elements have been accumulated and then pass the collection of elements downstream.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/grouped.html")
  (group-by
    [source-or-flow max-substreams grouping-fn]
    [source-or-flow max-substreams grouping-fn allow-closed-substream-recreation?]
    "Demultiplex the incoming stream into separate output streams.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupBy.html")
  (split-when
    [source-or-flow pred?]
    [source-or-flow ^SubstreamCancelStrategy substream-cancel-strategy pred?]
    "Split off elements into a new substream whenever a predicate function return true.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/splitWhen.html")
  (split-after
    [source-or-flow pred?]
    [source-or-flow ^SubstreamCancelStrategy substream-cancel-strategy pred?]
    "End the current substream whenever a predicate returns true, starting a new substream for the next element.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/splitAfter.html")
  (flatmap-concat [source-or-flow mapping-fn]
    "Transform each input element into a Source whose elements are then flattened into the output stream through concatenation
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/flatMapConcat.html")
  (flatmap-merge [source-or-flow breadth mapping-fn]
    "Transform each input element into a Source whose elements are then flattened into the output stream through merging.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/flatMapMerge.html")
  (merge
    [source-or-flow ^Graph stream]
    [source-or-flow ^Graph stream eager-complete?]
    "Merge multiple sources.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/merge.html")
  (merge-all [source-or-flow streams eager-complete?]
    "Merge multiple sources.
    https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/mergeAll.html"))

(defprotocol ISubStreamOperations
  (merge-substreams [sub-source-or-flow])
  (merge-substreams-with-parallelism [sub-source-or-flow parallelism])
  (concat-substreams [sub-source-or-flow])
  (also-to-all [sub-source-or-flow streams]))

(extend-type Flow
  IStreamOperations
  (map [this mapping-fn]
    (.map this (utils/->fn1 mapping-fn)))
  (mapcat [this mapping-fn]
    (.mapConcat this (utils/->fn1 mapping-fn)))
  (map-async
    ([this parallelism mapping-fn]
     (.mapAsync this (int parallelism)
                (utils/->fn1 (fn [arg]
                               (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))))))
    ([this parallelism mapping-fn then-fn]
     (.mapAsync this (int parallelism) (utils/->fn1 (fn [arg]
                                                      (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                          (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (map-async-unordered
    ([this parallelism mapping-fn]
     (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                               (CompletableFuture/supplyAsync
                                                                 (utils/->fn0 (fn [] (mapping-fn arg))))))))
    ([this parallelism mapping-fn then-fn]
     (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                               (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                                   (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (map-async-partitioned [this parallelism per-partition-count partitioner-fn bi-mapper-fn]
    (.mapAsyncPartitioned this (int parallelism) (int per-partition-count) (utils/->fn1 partitioner-fn) (utils/->fn2 bi-mapper-fn)))
  (map-materialized-value [this mapping-fn]
    (.mapMaterializedValue this (utils/->fn1 mapping-fn)))
  (filter [this pred?]
    (.filter this (utils/->fn1 pred?)))
  (filter-falsy [this pred?]
    (.filterNot this (utils/->fn1 (complement pred?))))
  (fold [this zero-value fold-fn]
    (.fold this zero-value (utils/->fn2 (fn [arg1 arg2] (fold-fn arg1 arg2)))))
  (fold-async
    ([this zero-value fold-fn]
     (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                                (CompletableFuture/supplyAsync
                                                  (utils/->fn0 (fn [] (fold-fn arg1 arg2))))))))
    ([this zero-value fold-fn then-fn]
     (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                                (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (fold-fn arg1 arg2))))
                                                    (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (concat [this ^Graph stream]
    (.concat this stream))
  (concat-lazy [this ^Graph stream]
    (.concatLazy this stream))
  (concat-all-lazy [this streams]
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
  (take-while
    ([this pred?]
     (.takeWhile this (utils/->fn1 pred?)))
    ([this pred? inclusive?]
     (.takeWhile this (utils/->fn1 pred?) inclusive?)))
  (sliding [this count step-size]
    (.sliding this (int count) (int step-size)))
  (grouped [this size]
    (.grouped this (int size)))
  (group-by
    ([this max-substreams grouping-fn]
     (.groupBy this (int max-substreams) (utils/->fn1 grouping-fn)))
    ([this max-substreams grouping-fn allow-closed-substream-recreation?]
     (.groupBy this (int max-substreams) (utils/->fn1 grouping-fn) allow-closed-substream-recreation?)))
  (split-when
    ([this pred?]
     (.splitWhen this pred?))
    ([this ^SubstreamCancelStrategy substream-cancel-strategy pred?]
     (.splitAfter this substream-cancel-strategy pred?)))
  (split-after
    ([this pred?]
     (.splitAfter this pred?))
    ([this ^SubstreamCancelStrategy substream-cancel-strategy pred?]
     (.splitAfter this substream-cancel-strategy pred?)))
  (flatmap-concat [this mapping-fn]
    (.flatMapConcat this (utils/->fn1 mapping-fn)))
  (flatmap-merge [this breadth mapping-fn]
    (.flatMapMerge this (int breadth) (utils/->fn1 mapping-fn)))
  (merge
    ([this ^Graph stream]
     (.merge this stream))
    ([this ^Graph stream eager-complete?]
     (.merge this stream eager-complete?)))
  (merge-all [this streams eager-complete?]
    (.mergeAll this ^List streams eager-complete?)))

(extend-type SubFlow
  IStreamOperations
  (map [this mapping-fn]
    (.map this (utils/->fn1 mapping-fn)))
  (mapcat [this mapping-fn]
    (.mapConcat this (utils/->fn1 mapping-fn)))
  (map-async
    ([this parallelism mapping-fn]
     (.mapAsync this (int parallelism)
                (utils/->fn1 (fn [arg]
                               (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))))))
    ([this parallelism mapping-fn then-fn]
     (.mapAsync this (int parallelism) (utils/->fn1 (fn [arg]
                                                      (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                          (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (map-async-unordered
    ([this parallelism mapping-fn]
     (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                               (CompletableFuture/supplyAsync
                                                                 (utils/->fn0 (fn [] (mapping-fn arg))))))))
    ([this parallelism mapping-fn then-fn]
     (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                               (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                                   (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (map-async-partitioned [this parallelism per-partition-count partitioner-fn bi-mapper-fn]
    (.mapAsyncPartitioned this (int parallelism) (int per-partition-count) (utils/->fn1 partitioner-fn) (utils/->fn2 bi-mapper-fn)))
  (filter [this pred?]
    (.filter this (utils/->fn1 pred?)))
  (filter-falsy [this pred?]
    (.filterNot this (utils/->fn1 (complement pred?))))
  (fold [this zero-value fold-fn]
    (.fold this zero-value (utils/->fn2 (fn [arg1 arg2] (fold-fn arg1 arg2)))))
  (fold-async
    ([this zero-value fold-fn]
     (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                                (CompletableFuture/supplyAsync
                                                  (utils/->fn0 (fn [] (fold-fn arg1 arg2))))))))
    ([this zero-value fold-fn then-fn]
     (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                                (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (fold-fn arg1 arg2))))
                                                    (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (concat [this ^Graph stream]
    (.concat this stream))
  (concat-lazy [this ^Graph stream]
    (.concatLazy this stream))
  (concat-all-lazy [this streams]
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
  (take-while
    ([this pred?]
     (.takeWhile this (utils/->fn1 pred?)))
    ([this pred? inclusive?]
     (.takeWhile this (utils/->fn1 pred?) inclusive?)))
  (sliding [this count step-size]
    (.sliding this (int count) (int step-size)))
  (flatmap-concat [this mapping-fn]
    (.flatMapConcat this (utils/->fn1 mapping-fn)))
  (flatmap-merge [this breadth mapping-fn]
    (.flatMapMerge this (int breadth) (utils/->fn1 mapping-fn)))
  (merge
    ([this ^Graph stream]
     (.merge this stream))
    ([this streams eager-complete?]
     (.mergeAll this ^List streams eager-complete?)))
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
  (map-async
    ([this parallelism mapping-fn]
     (.mapAsync this (int parallelism)
                (utils/->fn1 (fn [arg]
                               (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))))))
    ([this parallelism mapping-fn then-fn]
     (.mapAsync this (int parallelism) (utils/->fn1 (fn [arg]
                                                      (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                          (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (map-async-unordered
    ([this parallelism mapping-fn]
     (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                               (CompletableFuture/supplyAsync
                                                                 (utils/->fn0 (fn [] (mapping-fn arg))))))))
    ([this parallelism mapping-fn then-fn]
     (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                               (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                                   (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (map-async-partitioned [this parallelism per-partition-count partitioner-fn bi-mapper-fn]
    (.mapAsyncPartitioned this (int parallelism) (int per-partition-count) (utils/->fn1 partitioner-fn) (utils/->fn2 bi-mapper-fn)))
  (map-materialized-value [this mapping-fn]
    (.mapMaterializedValue this (utils/->fn1 mapping-fn)))
  (filter [this pred?]
    (.filter this (utils/->fn1 pred?)))
  (filter-falsy [this pred?]
    (.filterNot this (utils/->fn1 (complement pred?))))
  (fold [this zero-value fold-fn]
    (.fold this zero-value (utils/->fn2 (fn [arg1 arg2] (fold-fn arg1 arg2)))))
  (fold-async
    ([this zero-value fold-fn]
     (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                                (CompletableFuture/supplyAsync
                                                  (utils/->fn0 (fn [] (fold-fn arg1 arg2))))))))
    ([this zero-value fold-fn then-fn]
     (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                                (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (fold-fn arg1 arg2))))
                                                    (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (concat [this ^Graph stream]
    (.concat this stream))
  (concat-lazy [this ^Graph stream]
    (.concatLazy this stream))
  (concat-all-lazy [this streams]
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
  (take-while
    ([this pred?]
     (.takeWhile this (utils/->fn1 pred?)))
    ([this pred? inclusive?]
     (.takeWhile this (utils/->fn1 pred?) inclusive?)))
  (sliding [this count step-size]
    (.sliding this (int count) (int step-size)))
  (grouped [this size]
    (.grouped this (int size)))
  (group-by
    ([this max-substreams grouping-fn]
     (.groupBy this (int max-substreams) (utils/->fn1 grouping-fn)))
    ([this max-substreams grouping-fn allow-closed-substream-recreation?]
     (.groupBy this (int max-substreams) (utils/->fn1 grouping-fn) allow-closed-substream-recreation?)))
  (split-when
    ([this pred?]
     (.splitWhen this pred?))
    ([this ^SubstreamCancelStrategy substream-cancel-strategy pred?]
     (.splitAfter this substream-cancel-strategy pred?)))
  (split-after
    ([this pred?]
     (.splitAfter this pred?))
    ([this ^SubstreamCancelStrategy substream-cancel-strategy pred?]
     (.splitAfter this substream-cancel-strategy pred?)))
  (flatmap-concat [this mapping-fn]
    (.flatMapConcat this (utils/->fn1 mapping-fn)))
  (flatmap-merge [this breadth mapping-fn]
    (.flatMapMerge this (int breadth) (utils/->fn1 mapping-fn)))
  (merge
    ([this ^Graph stream]
     (.merge this stream))
    ([this ^Graph stream eager-complete?]
     (.merge this stream eager-complete?)))
  (merge-all [this streams eager-complete?]
    (.mergeAll this ^List streams eager-complete?)))

(extend-type SubSource
  IStreamOperations
  (map [this mapping-fn]
    (.map this (utils/->fn1 mapping-fn)))
  (mapcat [this mapping-fn]
    (.mapConcat this (utils/->fn1 mapping-fn)))
  (map-async
    ([this parallelism mapping-fn]
     (.mapAsync this (int parallelism)
                (utils/->fn1 (fn [arg]
                               (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))))))
    ([this parallelism mapping-fn then-fn]
     (.mapAsync this (int parallelism) (utils/->fn1 (fn [arg]
                                                      (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                          (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (map-async-unordered
    ([this parallelism mapping-fn]
     (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                               (CompletableFuture/supplyAsync
                                                                 (utils/->fn0 (fn [] (mapping-fn arg))))))))
    ([this parallelism mapping-fn then-fn]
     (.mapAsyncUnordered this (int parallelism) (utils/->fn1 (fn [arg]
                                                               (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (mapping-fn arg))))
                                                                   (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (map-async-partitioned [this parallelism per-partition-count partitioner-fn bi-mapper-fn]
    (.mapAsyncPartitioned this (int parallelism) (int per-partition-count) (utils/->fn1 partitioner-fn) (utils/->fn2 bi-mapper-fn)))
  (filter [this pred?]
    (.filter this (utils/->fn1 pred?)))
  (filter-falsy [this pred?]
    (.filterNot this (utils/->fn1 (complement pred?))))
  (fold [this zero-value fold-fn]
    (.fold this zero-value (utils/->fn2 (fn [arg1 arg2] (fold-fn arg1 arg2)))))
  (fold-async
    ([this zero-value fold-fn]
     (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                                (CompletableFuture/supplyAsync
                                                  (utils/->fn0 (fn [] (fold-fn arg1 arg2))))))))
    ([this zero-value fold-fn then-fn]
     (.foldAsync this zero-value (utils/->fn2 (fn [arg1 arg2]
                                                (-> (CompletableFuture/supplyAsync (utils/->fn0 (fn [] (fold-fn arg1 arg2))))
                                                    (.thenApplyAsync (utils/->fn1 (fn [arg] (then-fn arg))))))))))
  (concat [this ^Graph stream]
    (.concat this stream))
  (concat-lazy [this ^Graph stream]
    (.concatLazy this stream))
  (concat-all-lazy [this streams]
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
  (take-while
    ([this pred?]
     (.takeWhile this (utils/->fn1 pred?)))
    ([this pred? inclusive?]
     (.takeWhile this (utils/->fn1 pred?) inclusive?)))
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
    (.mapMaterializedValue this (utils/->fn1 mapping-fn)))
  (run [this materializer-or-actor-system]
    (condp instance? materializer-or-actor-system
      ActorSystem (.run this ^ActorSystem materializer-or-actor-system)
      Materializer (.run this ^Materializer materializer-or-actor-system))))

(defn source-from-graph
  "Returns Source from Graph"
  [^Graph graph]
  (Source/fromGraph graph))

(defn source-from-materializer
  "Returns Source from materializer"
  [f]
  (Source/fromMaterializer (utils/->fn2 f)))

(defn create-flow
  "Creates a Flow"
  []
  (Flow/create))

(defn flow-from-graph
  "Returns Flow from Graph"
  [^Graph graph]
  (Flow/fromGraph graph))

(defn flow-from-function
  "Returns Flow from function"
  [f]
  (Flow/fromFunction (utils/->fn1 f)))

(defn flow-from-sink-and-source
  "Returns flow from Source and Sink graphs"
  [^Graph sink ^Graph source]
  (Flow/fromSinkAndSource sink source))

(defn flow-from-materializer
  "Returns flow from materializer"
  [f]
  (Flow/fromMaterializer (utils/->fn2 f)))

(def ^{:doc "Sink that ignores incoming elements"} ignoring-sink (Sink/ignore))

(def ^{:doc "Sink that creates CompletionStage<List<IncomingElements>>"} sequence-sink (Sink/seq))

(defn sink-from-graph
  "Returns Sink from Graph"
  [^Graph graph]
  (Sink/fromGraph graph))
