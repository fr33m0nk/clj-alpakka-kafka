(ns fr33m0nk.clj-actor-test
  (:refer-clojure :exclude [map key])
  (:require [clojure.test :refer [deftest testing is]]
            [fr33m0nk.akka.actor :as actor]
            [fr33m0nk.utils :as utils]
            [fr33m0nk.akka.stream :as s]
            [fr33m0nk.alpakka-kafka.consumer :refer [drain-and-shutdown]]
            [fr33m0nk.alpakka-kafka.committer :refer [sink]]
            [fr33m0nk.alpakka-kafka.producer :refer [single-producer-message-envelope ->producer-record producer-message-passthrough]])
  (:import (akka.actor ActorSystem)
           (akka.japi Pair)
           (akka.kafka CommitterSettings ProducerMessage$Message)
           (akka.kafka.testkit ConsumerResultFactory ProducerResultFactory)
           (akka.kafka.testkit.javadsl ConsumerControlFactory)
           (akka.stream.javadsl Keep Source)
           (java.util.concurrent CompletableFuture)
           (org.apache.kafka.clients.consumer ConsumerRecord)))

(defn generate-consumer-messages
  ([group-id topic partition]
   (let [start-offset (atom 0)]
     (repeatedly (fn []
                   (let [next-offset (swap! start-offset inc)]
                     (ConsumerResultFactory/committableMessage
                       (ConsumerRecord. topic partition
                                        next-offset
                                        "key"
                                        (str "value " next-offset))
                       (ConsumerResultFactory/committableOffset
                         group-id topic partition next-offset (str "metadata " next-offset))))))))
  ([group-id topic partition n]
   (let [start-offset (atom 0)]
     (repeatedly n (fn []
                     (let [next-offset (swap! start-offset inc)]
                       (ConsumerResultFactory/committableMessage
                         (ConsumerRecord. topic partition
                                          next-offset
                                          "key"
                                          (str "value " next-offset))
                         (ConsumerResultFactory/committableOffset
                           group-id topic partition next-offset (str "metadata " next-offset)))))))))

(defn mocked-kafka-consumer-source
  [consumer-messages]
  (-> (Source/from consumer-messages)
      (s/via-mat (ConsumerControlFactory/controlFlow) (fn [_ b] b))))

(defn mocked-producer-flow
  []
  (-> (s/create-flow)
      (s/map (fn [message]
               (if (instance? ProducerMessage$Message message)
                 (do (println "producing message")
                     (ProducerResultFactory/result message))
                 (throw (Exception. (str "unexpected element: " message))))))))

(defn test-stream
  ^Pair
  [actor-system committer-settings target-topic]
  (-> (generate-consumer-messages "test-consumer-group" "test-topic" 0)
      (mocked-kafka-consumer-source)
      (s/map (fn [message]
               (println "mapping message")
               #_(single-producer-message-envelope (.committableOffset message)
                                                   (->producer-record target-topic (.. message (record) (key)) (.. message (record) (value))))
               (ProducerMessage$Message.
                 (->producer-record target-topic (.. message (record) (key)) (.. message (record) (value)))
                 (.committableOffset message))))
      (s/via (mocked-producer-flow))
      (s/map producer-message-passthrough)
      (s/to-mat (sink committer-settings) #(Pair/create %1 %2))
      (s/run actor-system)))



(comment

  (def actor-system (ActorSystem/create "test-system"))
  (def committer-settings (let [batch-size 1] (-> (CommitterSettings/create ^ActorSystem actor-system)
                                                  (.withMaxBatch batch-size))))

  (def run-test (test-stream actor-system committer-settings "test-target"))

  @(drain-and-shutdown (first (utils/pair->vector run-test))
                      (CompletableFuture/supplyAsync (utils/->fn0 (fn [] :done)))
                      (actor/get-dispatcher actor-system))

  @(actor/terminate actor-system)

  )
