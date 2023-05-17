(ns fr33m0nk.alpakka-kafka.transactional
  (:require [fr33m0nk.utils :as utils])
  (:import (akka.kafka ConsumerSettings ProducerSettings)
           (akka.kafka.javadsl Transactional)))

(defn ->transactional-source
  [^ConsumerSettings consumer-settings topics]
  (Transactional/source consumer-settings (utils/topics->subscriptions topics)))


(defn ->transactional-sink
  [^ProducerSettings producer-settings ^String transactional-id]
  (Transactional/source producer-settings transactional-id))

(defn transactional-flow
  [^ProducerSettings producer-settings ^String transactional-id]
  (Transactional/flow producer-settings transactional-id))

(defn transactional-flow-with-offset-context
  [^ProducerSettings producer-settings ^String transactional-id]
  (Transactional/flowWithOffsetContext producer-settings transactional-id))
