(ns fr33m0nk.alpakka-kafka.producer
  (:require [clojure.string :as str])
  (:import (akka.actor ActorSystem)
           (akka.kafka CommitterSettings ConsumerMessage$CommittableMessage ProducerMessage ProducerMessage$Envelope ProducerSettings ProducerMessage$Result ProducerMessage$Results)
           (akka.kafka.javadsl Producer SendProducer)
           (java.util Collection Map)
           (java.util.concurrent CompletionStage)
           (org.apache.kafka.clients.producer ProducerRecord)
           (org.apache.kafka.common.serialization Serializer)))

(defn producer-settings
  "Expects producer properties to be supplied with kebab-case-keyword keys
  Full config list can be found in org.apache.kafka.clients.producer.ProducerConfig"
  ^ProducerSettings
  [^ActorSystem actor-system {:keys [key-serializer value-serializer bootstrap-servers] :as producer-properties}]
  (let [producer-config (-> producer-properties
                            (dissoc :key-serializer :value-serializer :bootstrap-servers)
                            (update-keys (fn [key] (-> key name (str/replace #"-" ".")))))]
    (-> (ProducerSettings/create actor-system ^Serializer key-serializer ^Serializer value-serializer)
        (.withBootstrapServers bootstrap-servers)
        (.withProperties ^Map producer-config))))

(defprotocol IProducerMessage
  (producer-message-passthrough [this])
  (producer-message-with-passthrough [this passthrough]))

(extend-protocol IProducerMessage
  ProducerMessage$Envelope
  (producer-message-passthrough [this]
    (.passThrough this))
  (producer-message-with-passthrough [this passthrough]
    (.withPassThrough this passthrough))
  ProducerMessage$Result
  (producer-message-passthrough [this]
    (.passThrough this))
  ProducerMessage$Results
  (producer-message-passthrough [this]
    (.passThrough this)))

(defn flexi-flow
  [^ProducerSettings producer-settings]
  (Producer/flexiFlow producer-settings))

(defn flow-with-context
  [^ProducerSettings producer-settings]
  (Producer/flowWithContext producer-settings))

(defn plain-sink
  [^ProducerSettings producer-settings]
  (Producer/plainSink producer-settings))

(defn committable-sink
  [^ProducerSettings producer-settings ^CommitterSettings committer-settings]
  (Producer/committableSink producer-settings committer-settings))

(defn single-producer-message-envelope
  ^ProducerMessage$Envelope
  [message-offset ^ProducerRecord producer-record]
  (ProducerMessage/single producer-record message-offset))

(defn multi-producer-message-envelope
  ^ProducerMessage$Envelope
  [message-offset producer-records]
  (ProducerMessage/multi ^Collection producer-records message-offset))

(defn ->send-producer
  [^ProducerSettings producer-settings ^ActorSystem actor-system]
  (SendProducer. producer-settings actor-system))

(defn publish-via-send-producer
  ^CompletionStage
  [^SendProducer send-producer ^ProducerRecord producer-record]
  (.send send-producer producer-record))

(defn ->producer-record
  "creates a producer record which can be used later for publishing"
  ([topic value]
   (ProducerRecord. topic value))
  ([topic value key]
   (ProducerRecord. topic key value))
  ([topic value key partition-number]
   (ProducerRecord. topic (int partition-number) key value))
  ([topic value key partition-number timestamp-as-long ^Iterable headers]
   (ProducerRecord. topic (int partition-number) timestamp-as-long key value headers)))
