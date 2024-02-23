(ns fr33m0nk.alpakka-kafka.transactional
  "Akka Stream connector to support transactions between Kafka topics."
  (:require [fr33m0nk.alpakka-kafka.consumer :refer [topics->subscriptions]])
  (:import (akka.kafka ConsumerSettings ProducerSettings)
           (akka.kafka.javadsl Transactional)))

(defn ->transactional-source
  "Transactional source to setup a stream for Exactly Only Once (EoS) kafka message semantics.
  To enable EoS it's necessary to use the Transactional.sink or Transactional.flow (for passthrough).
  https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Transactional$.html#source[K,V](consumerSettings:akka.kafka.ConsumerSettings[K,V],subscription:akka.kafka.Subscription):akka.stream.javadsl.Source[akka.kafka.ConsumerMessage.TransactionalMessage[K,V],akka.kafka.javadsl.Consumer.Control]"
  [^ConsumerSettings consumer-settings topics]
  (Transactional/source consumer-settings (topics->subscriptions topics)))


(defn ->transactional-sink
  "Sink that is aware of the ConsumerMessage.TransactionalMessage.partitionOffset from a Transactional.source.
   It will initialize, begin, produce, and commit the consumer offset as part of a transaction.
   https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Transactional$.html#sink[K,V,IN%3C:akka.kafka.ProducerMessage.Envelope[K,V,akka.kafka.ConsumerMessage.PartitionOffset]](settings:akka.kafka.ProducerSettings[K,V],transactionalId:String):akka.stream.javadsl.Sink[IN,java.util.concurrent.CompletionStage[akka.Done]]"
  [^ProducerSettings producer-settings ^String transactional-id]
  (Transactional/sink producer-settings transactional-id))

(defn transactional-flow
  "Publish records to Kafka topics and then continue the flow.
  The flow can only be used with a Transactional.source that emits a ConsumerMessage.TransactionalMessage.
  The flow requires a unique transactional.id across all app instances.
  The flow will override producer properties to enable Kafka exactly-once transactional support.
  https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Transactional$.html#flow[K,V,IN%3C:akka.kafka.ProducerMessage.Envelope[K,V,akka.kafka.ConsumerMessage.PartitionOffset]](settings:akka.kafka.ProducerSettings[K,V],transactionalId:String):akka.stream.javadsl.Flow[IN,akka.kafka.ProducerMessage.Results[K,V,akka.kafka.ConsumerMessage.PartitionOffset],akka.NotUsed]"
  [^ProducerSettings producer-settings ^String transactional-id]
  (Transactional/flow producer-settings transactional-id))

(defn transactional-flow-with-offset-context
  "https://doc.akka.io/api/alpakka-kafka/5.0.0/akka/kafka/javadsl/Transactional$.html#flowWithOffsetContext[K,V](settings:akka.kafka.ProducerSettings[K,V],transactionalId:String):akka.stream.javadsl.FlowWithContext[akka.kafka.ProducerMessage.Envelope[K,V,akka.NotUsed],akka.kafka.ConsumerMessage.PartitionOffset,akka.kafka.ProducerMessage.Results[K,V,akka.kafka.ConsumerMessage.PartitionOffset],akka.kafka.ConsumerMessage.PartitionOffset,akka.NotUsed]"
  [^ProducerSettings producer-settings ^String transactional-id]
  (Transactional/flowWithOffsetContext producer-settings transactional-id))
