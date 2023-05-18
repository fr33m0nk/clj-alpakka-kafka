(ns fr33m0nk.alpakka-kafka.consumer
  (:require [clojure.string :as str]
            [fr33m0nk.utils :as utils])
  (:import (akka.actor ActorRef ActorSystem)
           (akka.japi Pair)
           (akka.kafka ConnectionCheckerSettings ConsumerMessage$CommittableOffset ConsumerMessage$PartitionOffset ConsumerMessage$TransactionalMessage ConsumerSettings Subscriptions ConsumerMessage$CommittableMessage)
           (akka.kafka.javadsl Consumer)
           (akka.stream.javadsl Source)
           (java.util Map Set)
           (java.util.concurrent TimeUnit)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.common.serialization Deserializer)
           (scala.concurrent.duration FiniteDuration)))

(defn topics->subscriptions
  ^Subscriptions
  [topics]
  (Subscriptions/topics ^Set (set topics)))

(defn ->consumer-settings
  "Settings for consumers. See akka.kafka.consumer section in reference.conf
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/ConsumerSettings.html
  - Expects consumer-properties to be supplied with kebab-case-keyword keys
    Full config list can be found in org.apache.kafka.clients.consumer.ConsumerConfig"
  ^ConsumerSettings
  [^ActorSystem actor-system {:keys [group-id key-deserializer value-deserializer bootstrap-servers auto-offset-reset enable-auto-commit]
                              :or {auto-offset-reset "latest"
                                   enable-auto-commit "false"}
                              :as consumer-properties}]
  (let [consumer-config (-> consumer-properties
                            (dissoc :group-id :key-deserializer :value-deserializer :bootstrap-servers :auto-offset-reset :enable-auto-commit)
                            (update-keys (fn [key] (-> key name (str/replace #"-" ".")))))]
    (-> (ConsumerSettings/create actor-system ^Deserializer key-deserializer ^Deserializer value-deserializer)
        (.withGroupId group-id)
        (.withBootstrapServers bootstrap-servers)
        (.withProperty ConsumerConfig/AUTO_OFFSET_RESET_CONFIG auto-offset-reset)
        (.withProperty ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG enable-auto-commit)
        (.withPartitionAssignmentStrategyCooperativeStickyAssignor)
        (.withConnectionChecker (ConnectionCheckerSettings. true 20 (FiniteDuration. 30 TimeUnit/SECONDS) 10.0))
        (.withProperties ^Map consumer-config))))

(defn ->committable-source
  "Akka Stream connector for subscribing to Kafka topics
  The committableSource makes it possible to commit offset positions to Kafka.
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#committableSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.javadsl.Source[akka.kafka.ConsumerMessage.CommittableMessage[K,V],akka.kafka.javadsl.Consumer.Control]"
  ^Source
  [^ConsumerSettings consumer-settings topics]
  (Consumer/committableSource consumer-settings (topics->subscriptions topics)))

(defn ->at-most-once-source
  "Akka Stream connector for subscribing to Kafka topics
  Convenience for 'at-most once delivery' semantics.
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#atMostOnceSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.javadsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.kafka.javadsl.Consumer.Control]"
  [^ConsumerSettings consumer-settings topics]
  (Consumer/atMostOnceSource consumer-settings (topics->subscriptions topics)))

(defn ->committable-external-source
  "Akka Stream connector for subscribing to Kafka topics
  The same as #plainExternalSource but with offset commit support.
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#committableExternalSource[K,V](consumer:akka.actor.ActorRef,subscription:akka.kafka.ManualSubscription,groupId:String,commitTimeout:scala.concurrent.duration.FiniteDuration):akka.stream.javadsl.Source[akka.kafka.ConsumerMessage.CommittableMessage[K,V],akka.kafka.javadsl.Consumer.Control]"
  [^ActorRef consumer topic-partitions ^String group-id ^FiniteDuration commit-timeout-in-nanos]
  (Consumer/committableExternalSource consumer
                                      (Subscriptions/assignment ^Set (set topic-partitions))
                                      group-id
                                      (FiniteDuration/fromNanos ^long commit-timeout-in-nanos)))

(defn ->committable-partitioned-source
  "Akka Stream connector for subscribing to Kafka topics
  The same as #plainPartitionedSource but with offset commit support.
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#committablePartitionedSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.AutoSubscription):akka.stream.javadsl.Source[akka.japi.Pair[org.apache.kafka.common.TopicPartition,akka.stream.javadsl.Source[akka.kafka.ConsumerMessage.CommittableMessage[K,V],akka.NotUsed]],akka.kafka.javadsl.Consumer.Control]"
  [^ConsumerSettings consumer-settings topics]
  (Consumer/committablePartitionedSource consumer-settings (topics->subscriptions topics)))

(defn ->commit-with-metadata-source
  "Akka Stream connector for subscribing to Kafka topics
  The commitWithMetadataSource makes it possible to add additional metadata (in the form of a string) when an offset is committed based on the record.
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#commitWithMetadataSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription,metadataFromRecord:java.util.function.Function[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],String]):akka.stream.javadsl.Source[akka.kafka.ConsumerMessage.CommittableMessage[K,V],akka.kafka.javadsl.Consumer.Control]"
  [^ConsumerSettings consumer-settings topics consumer-record-metadata-extractor]
  (Consumer/commitWithMetadataSource consumer-settings (topics->subscriptions topics) (utils/->fn1 consumer-record-metadata-extractor)))

(defn ->commit-with-metadata-partitioned-source
  "Akka Stream connector for subscribing to Kafka topics
  The same as #plainPartitionedSource but with offset commit with metadata support.
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#commitWithMetadataPartitionedSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.AutoSubscription,metadataFromRecord:java.util.function.Function[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],String]):akka.stream.javadsl.Source[akka.japi.Pair[org.apache.kafka.common.TopicPartition,akka.stream.javadsl.Source[akka.kafka.ConsumerMessage.CommittableMessage[K,V],akka.NotUsed]],akka.kafka.javadsl.Consumer.Control]"
  [^ConsumerSettings consumer-settings topics consumer-record-metadata-extractor]
  (Consumer/commitWithMetadataPartitionedSource consumer-settings (topics->subscriptions topics) (utils/->fn1 consumer-record-metadata-extractor)))

(defn ->plain-partitioned-source
  "Akka Stream connector for subscribing to Kafka topics
  The plainPartitionedSource is a way to track automatic partition assignment from kafka.
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#plainPartitionedSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.AutoSubscription):akka.stream.javadsl.Source[akka.japi.Pair[org.apache.kafka.common.TopicPartition,akka.stream.javadsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.NotUsed]],akka.kafka.javadsl.Consumer.Control]"
  [^ConsumerSettings consumer-settings topics]
  (Consumer/plainPartitionedSource consumer-settings ^Set (set topics)))

(defn ->plain-source
  "Akka Stream connector for subscribing to Kafka topics
  The plainSource emits ConsumerRecord elements (as received from the underlying KafkaConsumer). It has no support for committing offsets to Kafka. It can be used when the offset is stored externally or with auto-commit (note that auto-commit is by default disabled)
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#plainSource[K,V](settings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.javadsl.Source[org.apache.kafka.clients.consumer.ConsumerRecord[K,V],akka.kafka.javadsl.Consumer.Control]"
  [^ConsumerSettings consumer-settings topics]
  (Consumer/plainSource consumer-settings ^Set (set topics)))

(def
  ^{:doc "Combine control and a stream completion signal materialized values into one, so that the stream can be stopped in a controlled way without losing commits
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#createDrainingControl[T](c:akka.kafka.javadsl.Consumer.Control,mat:java.util.concurrent.CompletionStage[T]):akka.kafka.javadsl.Consumer.DrainingControl[T]"}
  create-draining-control (utils/->fn2 (fn [control mat] (Consumer/createDrainingControl control mat))))

(def
  ^{:doc "Combine control and a stream completion signal materialized values into one, so that the stream can be stopped in a controlled way without losing commits
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#createDrainingControl[T](pair:akka.japi.Pair[akka.kafka.javadsl.Consumer.Control,java.util.concurrent.CompletionStage[T]]):akka.kafka.javadsl.Consumer.DrainingControl[T]"}
  create-draining-control-with-pair (utils/->fn1 (fn [^Pair pair] (Consumer/createDrainingControl pair))))

(def
  ^{:doc "Combine control and a stream completion signal materialized values into one, so that the stream can be stopped in a controlled way without losing commits
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/javadsl/Consumer$.html#createNoopControl():akka.kafka.javadsl.Consumer.Control"}
  create-noop-control (Consumer/createNoopControl))

(defn committable-offset
  "ConsumerMessage is the Output element of committableSource.
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/ConsumerMessage$$CommittableMessage.html#committableOffset:akka.kafka.ConsumerMessage.CommittableOffset"
  ^ConsumerMessage$CommittableOffset
  [^ConsumerMessage$CommittableMessage committable-message]
  (.committableOffset committable-message))

(defn partition-offset
  "Output element of transactionalSource. The offset is automatically committed as by the Producer
  https://doc.akka.io/api/alpakka-kafka/4.0.2/akka/kafka/ConsumerMessage$$TransactionalMessage.html#partitionOffset:akka.kafka.ConsumerMessage.PartitionOffset"
  ^ConsumerMessage$PartitionOffset
  [^ConsumerMessage$TransactionalMessage transactional-message]
  (.partitionOffset transactional-message))
