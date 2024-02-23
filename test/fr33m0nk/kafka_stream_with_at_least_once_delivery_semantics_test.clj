(ns fr33m0nk.kafka-stream-with-at-least-once-delivery-semantics-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [fr33m0nk.akka.actor :as actor]
            [fr33m0nk.akka.stream :as s]
            [fr33m0nk.alpakka-kafka.committer :as committer]
            [fr33m0nk.alpakka-kafka.consumer :as consumer]
            [fr33m0nk.alpakka-kafka.producer :as producer]
            [fr33m0nk.utils :as utils]
            [fr33m0nk.test-utils :as tu])
  (:import (java.util.concurrent CompletableFuture)
           (org.apache.kafka.common.serialization StringSerializer StringDeserializer)))

(defn kafka-stream-producing-multiple-messages-with-at-least-once-semantics
  [actor-system consumer-settings committer-settings producer-settings consumer-topics producer-topic processing-fn]
  (-> (consumer/->committable-source consumer-settings consumer-topics)
      (s/map-async 2
                   (fn [message]
                     (let [_key (consumer/key message)
                           value (consumer/value message)
                           committable-offset (consumer/committable-offset message)
                           messages-to-publish (processing-fn producer-topic value)]
                       (producer/multi-producer-message-envelope committable-offset messages-to-publish))))
      (s/via (producer/flexi-flow producer-settings))
      (s/map producer/producer-message-passthrough)
      (s/to-mat (committer/sink committer-settings) consumer/create-draining-control)
      (s/run actor-system)))

(deftest kafka-stream-producing-multiple-messages-with-at-least-once-semantics-test
  (testing "kafka stream producing multiple messages with at least once semantics test"
    (tu/with-kafka-test-container
      in-topic out-topic bootstrap-servers
      (let [actor-system (actor/->actor-system "test-actor-system")
            committer-settings (committer/committer-settings actor-system {:batch-size 2})
            consumer-settings (consumer/consumer-settings-from-actor-system-config actor-system
                                                                                   {:consumer-config-key "akka.kafka.consumer"
                                                                                    :group-id "alpakka-consumer-group-1"
                                                                                    :key-deserializer (StringDeserializer.)
                                                                                    :value-deserializer (StringDeserializer.)
                                                                                    :bootstrap-servers bootstrap-servers})
            producer-settings (producer/producer-settings-from-actor-system-config actor-system
                                                                                   {:producer-config-key "akka.kafka.producer"
                                                                                    :key-serializer (StringSerializer.)
                                                                                    :value-serializer (StringSerializer.)
                                                                                    :bootstrap-servers bootstrap-servers})
            processing-fn (fn [producer-topic message]
                            (->> (repeat 3 message)
                                 (mapv #(producer/->producer-record producer-topic (str/upper-case %)))))
            consumer-control (kafka-stream-producing-multiple-messages-with-at-least-once-semantics
                               actor-system consumer-settings committer-settings producer-settings [in-topic] out-topic processing-fn)
            in-messages [{:key "key-1" :value "msg-1"} {:key "key-2" :value "msg-2"} {:key "key-3" :value "msg-3"}]
            expected-out (->> [{:key nil :value "MSG-1"} {:key nil :value "MSG-2"} {:key nil :value "MSG-3"}]
                              (into [] (comp (map #(repeat 3 %)) cat)))]
        (try
          (Thread/sleep 1000)
          (run! (fn [{:keys [key value]}] (tu/send-record bootstrap-servers in-topic key value)) in-messages)
          (Thread/sleep 1000)

          (is (= expected-out (tu/read-records bootstrap-servers out-topic 1000)))

          (finally
            @(consumer/drain-and-shutdown consumer-control
                                          (CompletableFuture/supplyAsync
                                            (utils/->fn0 (fn [] ::done)))
                                          (actor/get-dispatcher actor-system))
            @(actor/terminate actor-system)))))))
