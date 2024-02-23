(ns fr33m0nk.kafka-stream-with-error-handling
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest testing is]]
            [fr33m0nk.akka.actor :as actor]
            [fr33m0nk.akka.stream :as s]
            [fr33m0nk.alpakka-kafka.committer :as committer]
            [fr33m0nk.alpakka-kafka.consumer :as consumer]
            [fr33m0nk.alpakka-kafka.producer :as producer]
            [fr33m0nk.akka.restart-source :as restart]
            [fr33m0nk.utils :as utils]
            [fr33m0nk.test-utils :as tu])
  (:import (java.util.concurrent CompletableFuture)
           (org.apache.kafka.common.serialization StringSerializer StringDeserializer)))

(defn kafka-stream-with-error-handling
  [actor-system restart-settings consumer-settings committer-settings producer-settings consumer-topics producer-topic restart-count processing-fn]
  (let [restart-counter (atom 1)
        consumer-control-reference (atom consumer/create-noop-control)
        recovered-from-crashes (promise)]
    (-> (restart/->restart-source-on-failures-with-backoff
          restart-settings
          (fn []
            (-> (consumer/->committable-source consumer-settings consumer-topics)
                ;; Hack to gain access to underlying consumer control instance
                ;; reference https://github.com/akka/alpakka-kafka/issues/1291
                ;; reference https://doc.akka.io/docs/alpakka-kafka/current/errorhandling.html#restarting-the-stream-with-a-backoff-stage
                (s/map-materialized-value (fn [consumer-control] (swap! consumer-control-reference #(identity %2) consumer-control) consumer-control))
                (s/map (fn [message]
                         ;; throwing exception to simulate processing failure and stream-restart and incrementing restart-counter
                         (when (<= @restart-counter restart-count)
                           (throw (ex-info (str "Simulating processing failure - " @restart-counter) {:error-count (swap! restart-counter inc)})))
                         (deliver recovered-from-crashes true)
                         (let [_key (consumer/key message)
                               value (consumer/value message)
                               committable-offset (consumer/committable-offset message)
                               messages-to-publish (processing-fn producer-topic value)]
                           (producer/multi-producer-message-envelope committable-offset messages-to-publish))))
                (s/via (producer/flexi-flow producer-settings))
                (s/map producer/producer-message-passthrough)
                (s/via (committer/flow committer-settings)))))
        (s/run-with s/ignoring-sink actor-system))
    [consumer-control-reference recovered-from-crashes]))

(deftest kafka-stream-producing-multiple-messages-with-at-least-once-semantics-test
  (testing "kafka stream with error handling test"
    (tu/with-kafka-test-container
      in-topic out-topic bootstrap-servers
      (let [in-messages [{:key "key-1" :value "msg-1"} {:key "key-2" :value "msg-2"} {:key "key-3" :value "msg-3"}]
            _ (run! (fn [{:keys [key value]}] (tu/send-record bootstrap-servers in-topic key value)) in-messages)
            actor-system (actor/->actor-system "test-actor-system")
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
            restart-settings (restart/restart-settings 1000 1000 0.1 {})
            processing-fn (fn [producer-topic message]
                            (->> (repeat 3 message)
                                 (mapv #(producer/->producer-record producer-topic (str/upper-case %)))))
            [consumer-control-reference recovered-from-crashes]
            (kafka-stream-with-error-handling actor-system restart-settings consumer-settings
                                              committer-settings producer-settings
                                              [in-topic] out-topic 1 processing-fn)

            expected-out (->> [{:key nil :value "MSG-1"} {:key nil :value "MSG-2"} {:key nil :value "MSG-3"}]
                              (into [] (comp (map #(repeat 3 %)) cat)))]
        (try
          (loop []
            (when-not (realized? recovered-from-crashes)
              (recur)))
          (is (= expected-out (tu/read-records bootstrap-servers out-topic 240000)))

          (finally
            @(consumer/drain-and-shutdown @consumer-control-reference
                                          (CompletableFuture/supplyAsync
                                            (utils/->fn0 (fn [] ::done)))
                                          (actor/get-dispatcher actor-system))
            @(actor/terminate actor-system)))))))
