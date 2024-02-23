(ns fr33m0nk.alpakka-kafka.producer
  "Akka Stream connector for publishing messages to Kafka topics"
  (:require [clojure.string :as str])
  (:import (akka.actor ActorSystem)
           (akka.kafka CommitterSettings ProducerMessage ProducerMessage$Envelope ProducerSettings ProducerMessage$Result ProducerMessage$Results)
           (akka.kafka.javadsl Producer SendProducer)
           (java.util Collection Map)
           (java.util.concurrent CompletionStage)
           (org.apache.kafka.clients.producer ProducerRecord)
           (org.apache.kafka.common.serialization Serializer)))

(defn producer-settings
  "Settings for producers. See akka.kafka.producer section in reference.conf
  https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/ProducerSettings.html
  - Expects producer-properties to be supplied with kebab-case-keyword keys
    Full config list can be found in org.apache.kafka.clients.producer.ProducerConfig"
  ^ProducerSettings
  [^ActorSystem actor-system {:keys [key-serializer value-serializer bootstrap-servers] :as producer-properties}]
  (let [producer-config (-> producer-properties
                            (dissoc :key-serializer :value-serializer :bootstrap-servers)
                            (update-keys (fn [key] (-> key name (str/replace #"-" ".")))))]
    (-> (ProducerSettings/create actor-system ^Serializer key-serializer ^Serializer value-serializer)
        (.withBootstrapServers bootstrap-servers)
        (.withProperties ^Map producer-config))))

(defn producer-settings-from-actor-system-config
  "Settings for producers. See akka.kafka.producer section in reference.conf
  https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/ProducerSettings.html
  - Expects producer-properties to be supplied with kebab-case-keyword keys
    Full config list can be found in org.apache.kafka.clients.producer.ProducerConfig"
  ^ProducerSettings
  [^ActorSystem actor-system {:keys [producer-config-key key-serializer value-serializer bootstrap-servers]}]
  (let [producer-config (-> actor-system .settings .config (.getConfig producer-config-key))]
    (-> (ProducerSettings/create producer-config ^Serializer key-serializer ^Serializer value-serializer)
        (.withBootstrapServers bootstrap-servers))))

(defprotocol IProducerMessage
  "PassThroughMessage does not publish anything, and continues in the stream as PassThroughResult"
  (producer-message-passthrough [producer-message-envelope] "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/ProducerMessage$$Envelope.html#passThrough:PassThrough")
  (producer-message-with-passthrough [producer-message-envelope passthrough] "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/ProducerMessage$$Envelope.html#withPassThrough[PassThrough2](value:PassThrough2):akka.kafka.ProducerMessage.Envelope[K,V,PassThrough2]"))

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
  "Akka Stream connector for publishing messages to Kafka topics.
  https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Producer$.html#flexiFlow[K,V,PassThrough](settings:akka.kafka.ProducerSettings[K,V]):akka.stream.javadsl.Flow[akka.kafka.ProducerMessage.Envelope[K,V,PassThrough],akka.kafka.ProducerMessage.Results[K,V,PassThrough],akka.NotUsed]"
  [^ProducerSettings producer-settings]
  (Producer/flexiFlow producer-settings))

(defn flow-with-context
  "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Producer$.html#flowWithContext[K,V,C](settings:akka.kafka.ProducerSettings[K,V]):akka.stream.javadsl.FlowWithContext[akka.kafka.ProducerMessage.Envelope[K,V,akka.NotUsed],C,akka.kafka.ProducerMessage.Results[K,V,C],C,akka.NotUsed]"
  [^ProducerSettings producer-settings]
  (Producer/flowWithContext producer-settings))

(defn plain-sink
  "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Producer$.html#plainSink[K,V](settings:akka.kafka.ProducerSettings[K,V]):akka.stream.javadsl.Sink[org.apache.kafka.clients.producer.ProducerRecord[K,V],java.util.concurrent.CompletionStage[akka.Done]]"
  [^ProducerSettings producer-settings]
  (Producer/plainSink producer-settings))

(defn committable-sink
  "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Producer$.html#committableSink[K,V,IN%3C:akka.kafka.ProducerMessage.Envelope[K,V,akka.kafka.ConsumerMessage.Committable]](producerSettings:akka.kafka.ProducerSettings[K,V],committerSettings:akka.kafka.CommitterSettings):akka.stream.javadsl.Sink[IN,java.util.concurrent.CompletionStage[akka.Done]]"
  [^ProducerSettings producer-settings ^CommitterSettings committer-settings]
  (Producer/committableSink producer-settings committer-settings))

(defn committable-sink-with-offset-context
  "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Producer$.html#committableSinkWithOffsetContext[K,V,IN%3C:akka.kafka.ProducerMessage.Envelope[K,V,_],C%3C:akka.kafka.ConsumerMessage.Committable](producerSettings:akka.kafka.ProducerSettings[K,V],committerSettings:akka.kafka.CommitterSettings):akka.stream.javadsl.Sink[akka.japi.Pair[IN,C],java.util.concurrent.CompletionStage[akka.Done]]"
  [^ProducerSettings producer-settings ^CommitterSettings committer-settings]
  (Producer/committableSinkWithOffsetContext producer-settings committer-settings))

(defn single-producer-message-envelope
  "Message publishes a single message to its topic, and continues in the stream as Result
  https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/ProducerMessage$$Message.html"
  ^ProducerMessage$Envelope
  [message-offset ^ProducerRecord producer-record]
  (ProducerMessage/single producer-record message-offset))

(defn multi-producer-message-envelope
  "MultiMessage publishes all messages in its records field, and continues in the stream as MultiResult
  https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/ProducerMessage$$MultiMessage.html"
  ^ProducerMessage$Envelope
  [message-offset producer-records]
  (ProducerMessage/multi ^Collection producer-records message-offset))

(defn ->send-producer
  "Utility Kafka Producer for producing to Kafka without using Akka Streams.
  https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/SendProducer.html"
  [^ProducerSettings producer-settings ^ActorSystem actor-system]
  (SendProducer. producer-settings actor-system))

(defn publish-via-send-producer
  "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/SendProducer.html#send(record:org.apache.kafka.clients.producer.ProducerRecord[K,V]):java.util.concurrent.CompletionStage[org.apache.kafka.clients.producer.RecordMetadata]"
  ^CompletionStage
  [^SendProducer send-producer ^ProducerRecord producer-record]
  (.send send-producer producer-record))

(defn close-send-producer
  "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/SendProducer.html#close():java.util.concurrent.CompletionStage[akka.Done]"
  ^CompletionStage
  [^SendProducer send-producer]
  (.close send-producer))

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
