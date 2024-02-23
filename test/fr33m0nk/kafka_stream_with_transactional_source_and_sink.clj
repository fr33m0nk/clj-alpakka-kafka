(ns fr33m0nk.kafka-stream-with-transactional-source-and-sink
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [fr33m0nk.akka.actor :as actor]
            [fr33m0nk.akka.stream :as s]
            [fr33m0nk.alpakka-kafka.consumer :as consumer]
            [fr33m0nk.alpakka-kafka.producer :as producer]
            [fr33m0nk.akka.restart-source :as restart]
            [fr33m0nk.utils :as utils]
            [fr33m0nk.test-utils :as ftu]
            [fr33m0nk.alpakka-kafka.transactional :as transactional])
  (:import (java.util.concurrent CompletableFuture)
           (org.apache.kafka.common.serialization StringSerializer StringDeserializer)))

(defn kafka-stream-with-transactional-source-and-sink
  [actor-system restart-settings consumer-settings producer-settings consumer-topics producer-topic restart-count processing-fn]
  (let [consumer-control-reference (atom consumer/create-noop-control)]
    (-> (restart/->restart-source-on-failures-with-backoff
          restart-settings
          (fn []
            (-> (transactional/->transactional-source consumer-settings consumer-topics)
                ;; Hack to gain access to underlying consumer control instance
                ;; reference https://github.com/akka/alpakka-kafka/issues/1291
                ;; reference https://doc.akka.io/docs/alpakka-kafka/current/errorhandling.html#restarting-the-stream-with-a-backoff-stage
                (s/map-materialized-value (fn [consumer-control] (swap! consumer-control-reference #(identity %2) consumer-control) consumer-control))
                (s/map-async 2 (fn [message]
                                 (let [_key (consumer/key message)
                                       value (consumer/value message)
                                       partition-offset (consumer/partition-offset message)
                                       messages-to-publish (->> (repeat 3 value)
                                                                (mapv #(producer/->producer-record producer-topic (str/upper-case %))))]
                                   (producer/multi-producer-message-envelope partition-offset messages-to-publish))))
                (s/via (transactional/transactional-flow producer-settings "unique-transaction-id-for-this-application")))))
        (s/run-with s/ignoring-sink actor-system))
    consumer-control-reference))

(deftest kafka-stream-with-transactional-source-and-sink-test
  (testing "kafka stream with transactional source and sink test"
    (ftu/with-kafka-test-container
      in-topic out-topic bootstrap-servers
      (let [actor-system (actor/->actor-system "test-actor-system")
            consumer-settings (consumer/consumer-settings actor-system
                                                          {:group-id "alpakka-consumer"
                                                           :bootstrap-servers bootstrap-servers
                                                           :key-deserializer (StringDeserializer.)
                                                           :value-deserializer (StringDeserializer.)
                                                           :enable-auto-commit "false"})
            producer-settings (producer/producer-settings actor-system {:bootstrap-servers bootstrap-servers
                                                                        :key-serializer (StringSerializer.)
                                                                        :value-serializer (StringSerializer.)})
            restart-settings (restart/restart-settings 1000 5000 0.2 {})
            processing-fn (fn [producer-topic message]
                            (->> (repeat 3 message)
                                 (mapv #(producer/->producer-record producer-topic (str/upper-case %)))))
            consumer-control-reference (transactional-kafka-stream-with-error-handling
                                         actor-system restart-settings consumer-settings
                                         producer-settings [in-topic] out-topic 1 processing-fn)
            in-messages [{:key "key-1" :value "msg-1"} {:key "key-2" :value "msg-2"} {:key "key-3" :value "msg-3"}]
            expected-out (->> [{:key nil :value "MSG-1"} {:key nil :value "MSG-2"} {:key nil :value "MSG-3"}]
                              (into [] (comp (map #(repeat 3 %)) cat)))]
        (try
          (Thread/sleep 1000)
          (run! (fn [{:keys [key value]}] (ftu/send-record bootstrap-servers in-topic key value)) in-messages)

          (is (= expected-out (ftu/read-records bootstrap-servers out-topic 1000)))

          (finally
            @(consumer/drain-and-shutdown @consumer-control-reference
                                          (CompletableFuture/supplyAsync
                                            (utils/->fn0 (fn [] ::done)))
                                          (actor/get-dispatcher actor-system))
            @(actor/terminate actor-system)))))))
