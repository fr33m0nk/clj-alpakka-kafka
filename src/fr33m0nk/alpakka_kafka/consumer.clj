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
  ^Source
  [^ConsumerSettings consumer-settings topics]
  (Consumer/committableSource consumer-settings (topics->subscriptions topics)))

(defn ->at-most-once-source
  [^ConsumerSettings consumer-settings topics]
  (Consumer/atMostOnceSource consumer-settings (topics->subscriptions topics)))

(defn ->committable-external-source
  [^ActorRef consumer topic-partitions ^String group-id ^FiniteDuration commit-timeout-in-nanos]
  (Consumer/committableExternalSource consumer
                                      (Subscriptions/assignment ^Set (set topic-partitions))
                                      group-id
                                      (FiniteDuration/fromNanos ^long commit-timeout-in-nanos)))

(defn ->committable-partitioned-source
  [^ConsumerSettings consumer-settings topics]
  (Consumer/committablePartitionedSource consumer-settings (topics->subscriptions topics)))

(defn ->commit-with-metadata-source
  [^ConsumerSettings consumer-settings topics consumer-record-metadata-extractor]
  (Consumer/commitWithMetadataSource consumer-settings (topics->subscriptions topics) (utils/->fn1 consumer-record-metadata-extractor)))

(defn ->commit-with-metadata-partitioned-source
  [^ConsumerSettings consumer-settings topics consumer-record-metadata-extractor]
  (Consumer/commitWithMetadataPartitionedSource consumer-settings (topics->subscriptions topics) (utils/->fn1 consumer-record-metadata-extractor)))

(defn ->plain-partitioned-source
  [^ConsumerSettings consumer-settings topics]
  (Consumer/plainPartitionedSource consumer-settings ^Set (set topics)))

(defn ->plain-source
  [^ConsumerSettings consumer-settings topics]
  (Consumer/plainSource consumer-settings ^Set (set topics)))

(def create-draining-control (utils/->fn2 (fn [control mat] (Consumer/createDrainingControl control mat))))

(def create-draining-control-with-pair (utils/->fn1 (fn [^Pair pair] (Consumer/createDrainingControl pair))))

(def create-noop-control (Consumer/createNoopControl))

(defn committable-offset
  ^ConsumerMessage$CommittableOffset
  [^ConsumerMessage$CommittableMessage committable-message]
  (.committableOffset committable-message))

(defn partition-offset
  ^ConsumerMessage$PartitionOffset
  [^ConsumerMessage$TransactionalMessage transactional-message]
  (.partitionOffset transactional-message))
