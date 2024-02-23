(ns fr33m0nk.kafka-stream-sink-test
  (:require [clojure.test :refer [deftest testing is]]
            [fr33m0nk.akka.actor :as actor]
            [fr33m0nk.akka.stream :as s]
            [fr33m0nk.alpakka-kafka.committer :as committer]
            [fr33m0nk.alpakka-kafka.consumer :as consumer]
            [fr33m0nk.utils :as utils]
            [fr33m0nk.test-utils :as tu])
  (:import (java.util.concurrent CompletableFuture)
           (org.apache.kafka.common.serialization StringDeserializer)))

(defn kafka-stream-sink-only
  [actor-system consumer-settings committer-settings consumer-topics processing-fn]
  (-> (consumer/->committable-source consumer-settings consumer-topics)
      (s/map-async 2
                   (fn [message]
                     ;; Do business processing
                     (processing-fn message)
                     ;; Return message
                     message)
                   ;; then-fn returns committable offset for offset committing
                   (fn [message]
                     (consumer/committable-offset message)))
      (s/to-mat (committer/sink committer-settings) consumer/create-draining-control)
      (s/run actor-system)))

(deftest kafka-stream-sink-only-test
  (testing "kafka stream sink test"
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
            actual-messages (atom [])
            processing-fn (fn [message]
                            (let [key (consumer/key message)
                                  value (consumer/value message)
                                  processed {:key key :value value}]
                              (swap! actual-messages conj processed)))
            consumer-control (kafka-stream-sink-only actor-system consumer-settings committer-settings [in-topic] processing-fn)
            expected [{:key "key-1" :value "msg-1"} {:key "key-2" :value "msg-2"} {:key "key-3" :value "msg-3"}]]
        (try
          (Thread/sleep 1000)
          (run! (fn [{:keys [key value]}] (tu/send-record bootstrap-servers in-topic key value)) expected)
          (Thread/sleep 1000)
          (is (= expected @actual-messages))
          (finally
            @(consumer/drain-and-shutdown consumer-control
                                          (CompletableFuture/supplyAsync
                                            (utils/->fn0 (fn [] ::done)))
                                          (actor/get-dispatcher actor-system))
            @(actor/terminate actor-system)))))))
