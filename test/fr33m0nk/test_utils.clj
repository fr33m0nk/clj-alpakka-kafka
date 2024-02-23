(ns fr33m0nk.test-utils
  (:require
    [clj-test-containers.core :as tc]
    [taoensso.timbre :as log])
  (:import (java.time Duration)
           (org.apache.kafka.clients.admin AdminClient AdminClientConfig KafkaAdminClient NewTopic)
           (org.apache.kafka.clients.consumer ConsumerConfig ConsumerRecord KafkaConsumer)
           (org.apache.kafka.common.internals Topic)
           (org.apache.kafka.common.serialization StringSerializer StringDeserializer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord ProducerConfig)
           (org.testcontainers.containers KafkaContainer)
           (org.testcontainers.utility DockerImageName)))
(def kafka-docker-image "confluentinc/cp-kafka:7.4.0")

(defn kafka-test-container
  []
  (->
    (KafkaContainer. (DockerImageName/parse kafka-docker-image))
    (.withKraft)))

(defn create-topics
  [bootstrap-servers & topics]
  (with-open [admin-client (KafkaAdminClient/create {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers})]
    (let [new-topics (into [] (map #(NewTopic. % (int 1) (short 1))) (set topics))]
      @(.all (.createTopics admin-client new-topics)))))

(defn producer
  [bootstrap-servers]
  (KafkaProducer. {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers
                   ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"
                   ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"}))

(defn consumer
  [bootstrap-servers]
  (KafkaConsumer. {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers
                   ConsumerConfig/GROUP_ID_CONFIG "test-consumer"
                   ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                   ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"}))

(defn send-record
  ([bootstrap-servers topic value]
   (with-open [test-producer (producer bootstrap-servers)]
     (let [producer-record (ProducerRecord. topic value)]
       @(.send test-producer producer-record))))
  ([bootstrap-servers topic key value]
   (with-open [test-producer (producer bootstrap-servers)]
     (let [producer-record (ProducerRecord. topic key value)]
       @(.send test-producer producer-record)))))

(defn read-records
  [bootstrap-servers topic timeout-in-ms]
  (with-open [test-consumer (consumer bootstrap-servers)]
    (.subscribe test-consumer #{topic})
    (->> (.records (.poll ^KafkaConsumer test-consumer (Duration/ofMillis timeout-in-ms)) ^String topic)
         (into [] (map (fn [^ConsumerRecord cr]
                         {:key (.key cr)
                          :value (.value cr)}))))))

(defmacro with-kafka-test-container
  [in-topic out-topic bootstrap-servers & body]
  `(let [~in-topic "incoming"
         ~out-topic "outgoing"
         kafka-container# (tc/start! {:container (kafka-test-container)})
         ~bootstrap-servers (.getBootstrapServers (:container kafka-container#))
         topics# (create-topics ~bootstrap-servers ~in-topic ~out-topic)]
     (try

       ~@body
       (catch Exception ex#
         (log/error "Something went wrong while using Kafka test container" ex#))
       (finally
         (tc/stop! kafka-container#)
         (tc/perform-cleanup!)))))
