(ns fr33m0nk.kafka-stream-producing-multiple-messages-per-consumed-message
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [fr33m0nk.akka.actor :as actor]
            [fr33m0nk.akka.stream :as s]
            [fr33m0nk.alpakka-kafka.committer :as committer]
            [fr33m0nk.alpakka-kafka.consumer :as consumer]
            [fr33m0nk.alpakka-kafka.producer :as producer]
            [fr33m0nk.utils :as utils]
            [fr33m0nk.test-utils :as ftu])
  (:import (java.util.concurrent CompletableFuture)
           (org.apache.kafka.common.serialization StringSerializer StringDeserializer)))

(defn kafka-stream-producing-multiple-messages-per-consumed-message
  [actor-system consumer-settings committer-settings producer-settings consumer-topics producer-topic processing-fn]
  (-> (consumer/->committable-source consumer-settings consumer-topics)
      (s/map-async 2
                   (fn [message]
                     (let [_key (consumer/key message)
                           value (consumer/value message)
                           committable-offset (consumer/committable-offset message)
                           messages-to-publish (processing-fn producer-topic value)]
                       (producer/multi-producer-message-envelope committable-offset messages-to-publish))))
      (s/to-mat (producer/committable-sink producer-settings committer-settings) consumer/create-draining-control)
      (s/run actor-system)))

(deftest kafka-stream-with-producer-test
  (testing "kafka stream producing multiple messages per consumed message test"
    (ftu/with-kafka-test-container in-topic out-topic bootstrap-servers
      (let [actor-system (actor/->actor-system "test-actor-system")
            committer-settings (committer/committer-settings actor-system {:batch-size 2})
            consumer-settings (consumer/consumer-settings actor-system
                                                          {:group-id "alpakka-consumer"
                                                           :bootstrap-servers bootstrap-servers
                                                           :key-deserializer (StringDeserializer.)
                                                           :value-deserializer (StringDeserializer.)})
            producer-settings (producer/producer-settings actor-system {:bootstrap-servers bootstrap-servers
                                                                        :key-serializer (StringSerializer.)
                                                                        :value-serializer (StringSerializer.)})
            processing-fn (fn [producer-topic message]
                            (->> (repeat 5 message)
                                 (mapv #(producer/->producer-record producer-topic (str/upper-case %)))))
            consumer-control (kafka-stream-producing-multiple-messages-per-consumed-message
                               actor-system consumer-settings committer-settings producer-settings [in-topic] out-topic processing-fn)
            in-messages [{:key "key-1" :value "msg-1"} {:key "key-2" :value "msg-2"} {:key "key-3" :value "msg-3"}]
            expected-out (->> [{:key nil :value "MSG-1"} {:key nil :value "MSG-2"} {:key nil :value "MSG-3"}]
                              (into [] (comp (map #(repeat 5 %)) cat)))]
        (try
          (Thread/sleep 1000)
          (run! (fn [{:keys [key value]}] (ftu/send-record bootstrap-servers in-topic key value)) in-messages)
          (Thread/sleep 1000)

          (is (= expected-out (ftu/read-records bootstrap-servers out-topic 1000)))

          (finally
            @(consumer/drain-and-shutdown consumer-control
                                          (CompletableFuture/supplyAsync
                                            (utils/->fn0 (fn [] ::done)))
                                          (actor/get-dispatcher actor-system))
            @(actor/terminate actor-system)))))))
