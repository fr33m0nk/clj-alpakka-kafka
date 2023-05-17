(ns fr33m0nk.alpakka-kafka
  (:require
    [clojure.string :as str]
    [taoensso.timbre :as timbre
     :refer [log trace debug info warn error fatal report
             logf tracef debugf infof warnf errorf fatalf reportf
             spy]])
  (:import
    (akka.actor ActorSystem)
    (akka.dispatch Envelope)
    (akka.japi.function Creator Function Function2)
    (akka.kafka CommitterSettings ConnectionCheckerSettings ConsumerMessage$CommittableMessage ConsumerSettings
                ProducerMessage ProducerMessage$MultiResult ProducerSettings Subscriptions)
    (akka.kafka.javadsl Committer Consumer Consumer$Control Consumer$DrainingControl Producer)
    (akka.stream Graph RestartSettings)
    (akka.stream.javadsl RestartSource RunnableGraph Sink Source Flow)
    (akka.stream.scaladsl RestartWithBackoffSource)
    (com.typesafe.config Config)
    (java.time Duration)
    (java.util Collection Map Set)
    (java.util.concurrent CompletableFuture CompletionStage Executors TimeUnit)
    (java.util.concurrent.atomic AtomicReference)
    (java.util.function Supplier)
    (org.apache.kafka.clients.consumer ConsumerConfig ConsumerRecord)
    (org.apache.kafka.clients.producer ProducerRecord)
    (org.apache.kafka.common.serialization Deserializer Serializer StringSerializer StringSerializer StringDeserializer)
    (scala.concurrent.duration FiniteDuration)))


(defn ->actor-system
  ^ActorSystem
  [name]
  (ActorSystem/create name))

(defn config-from-actor-system
  [actor-system config-name]
  (.. actor-system (settings) (config) (getConfig config-name)))

(defn ->consumer-settings
  ^ConsumerSettings
  [^ActorSystem actor-system ^String consumer-group-id ^Deserializer key-deserializer ^Deserializer value-deserializer ^String auto-commit-offset-reset]
  (-> (ConsumerSettings/create actor-system key-deserializer value-deserializer)
      (.withGroupId consumer-group-id)
      (.withBootstrapServers "localhost:9092")
      (.withProperty ConsumerConfig/AUTO_OFFSET_RESET_CONFIG auto-commit-offset-reset)
      (.withProperty ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG "false")
      (.withPartitionAssignmentStrategyCooperativeStickyAssignor)
      (.withConnectionChecker (ConnectionCheckerSettings. true 20 (FiniteDuration. 30 TimeUnit/SECONDS) 10.0))
      (.withProperty ConsumerConfig/AUTO_COMMIT_INTERVAL_MS_CONFIG "5000")))

(defn ->committer-settings
  ^CommitterSettings
  [^ActorSystem actor-system batch-size]
  (-> (CommitterSettings/create actor-system)
      (.withMaxBatch batch-size)))

(defn ->committable-source
  ^Source
  [^ConsumerSettings consumer-settings & topics]
  (Consumer/committableSource consumer-settings (Subscriptions/topics ^Set (set topics))))

(defn ->committer-sink
  ^Sink
  [^CommitterSettings committer-settings]
  (Committer/sink committer-settings))

(defn ->async-mapping-function
  ^Function
  [kafka-record-mapping-function]
  (reify Function
    (apply [_this message]
      (let [kafka-record (.record ^ConsumerMessage$CommittableMessage message)]
        (-> (CompletableFuture/supplyAsync
              (reify Supplier
                (get [_this]
                  (kafka-record-mapping-function kafka-record))))
            (.thenApply (reify java.util.function.Function
                          (apply
                            ^CompletableFuture
                            [_this _arg]
                            (.committableOffset ^ConsumerMessage$CommittableMessage message)))))))))

(defn map-async
  ^Source
  [^Source source parallelism kafka-record-mapping-function]
  (.mapAsync source (int parallelism) (->async-mapping-function kafka-record-mapping-function)))

(def ->draining-control
  (reify
    Function2
    (apply [_this control mat]
      (Consumer/createDrainingControl control mat))))

(defn to-mat
  ^RunnableGraph
  [^Source source ^Graph sink]
  (.toMat source sink ->draining-control))

(defn run
  ^Consumer$DrainingControl
  [^RunnableGraph runnable-graph ^ActorSystem actor-system]
  (.run runnable-graph actor-system))

(defn shutdown
  ^CompletionStage
  [^Consumer$DrainingControl drain-control]
  (-> (.drainAndShutdown drain-control (Executors/newCachedThreadPool))
      (.thenApply (reify
                    java.util.function.Function
                    (apply [_this arg]
                      (info "Shutdown is complete")
                      arg)))))

(defn ->producer-settings
  ^ProducerSettings
  [^ActorSystem actor-system ^Serializer key-serializer ^Serializer value-serializer ^Map producer-properties]
  (-> (ProducerSettings/create actor-system key-serializer value-serializer)
      (.withBootstrapServers "localhost:9092")
      (.withProperties producer-properties)))

(defn ->producer-flexiflow
  ^Flow
  [^ProducerSettings producer-settings]
  (Producer/flexiFlow producer-settings))

(defn via-flow
  ^Source
  [^Source source ^Graph flow]
  (.via source flow))

(defn map-sync
  [^Source source mapping-fn]
  (.map source (reify
                 Function
                 (apply [_this arg]
                   (mapping-fn arg)))))

(defn consumer-record->producer-record
  ([^ConsumerRecord record ^String output-topic value-mapping-fn]
   (consumer-record->producer-record record output-topic value-mapping-fn identity))
  ([^ConsumerRecord record ^String output-topic value-mapping-fn key-mapping-fn]
   (let [record-key (.key record)
         record-value (.value record)
         mapped-key (key-mapping-fn record-key)
         mapped-value (value-mapping-fn record-value)]
     (ProducerRecord. output-topic mapped-key mapped-value))))

(defn consumer-record->producer-record-envelope-fn
  [consumer-record->producer-record-fn]
  (fn consumer-record->producer-record-envelope
    ^Envelope
    [^ConsumerMessage$CommittableMessage message]
    (let [consumer-record (.record message)
          ^ProducerRecord producer-record (consumer-record->producer-record-fn consumer-record)]
      (ProducerMessage/single producer-record (.committableOffset message)))))

(defn consumer-record->producer-records-envelope-fn
  [consumer-record->producer-records-fn]
  (fn consumer-record->producer-record-envelope
    ^Envelope
    [^ConsumerMessage$CommittableMessage message]
    (let [consumer-record (.record message)
          ^Collection producer-records (consumer-record->producer-records-fn consumer-record)]
      (ProducerMessage/multi producer-records (.committableOffset message)))))



(defn map-sync-with-passthrough
  [^Source source]
  (.map source (reify
                 Function
                 (apply [_this producer-message-multi-result]
                   (.passThrough ^ProducerMessage$MultiResult producer-message-multi-result)))))


(defn producer-committable-sink
  ^Sink
  [^ProducerSettings producer-settings ^CommitterSettings committer-settings]
  (Producer/committableSink producer-settings committer-settings))

(defn ->restart-settings
  ^RestartSettings
  [min-backoff-in-sec max-backoff-in-sec random-factor]
  (RestartSettings/create (Duration/ofSeconds min-backoff-in-sec) (Duration/ofSeconds max-backoff-in-sec) random-factor))


(defmacro ->restart-source
  [^RestartSettings restart-settings ^Source committable-source control-reference-atom restart-source & body]
  `(RestartSource/onFailuresWithBackoff
     ~restart-settings
     (reify
       Creator
       (create [_this]
         (let [~restart-source (.mapMaterializedValue ~committable-source (reify
                                                                            Function
                                                                            (apply [_this consumer-control#]
                                                                              (swap! ~control-reference-atom identity consumer-control#)
                                                                              consumer-control#)))]
           ~@body)))))

(defn consumer-record->producer-records-fn
  [topic]
  (fn [^ConsumerRecord consumer-record]
    (let [record-key (.key consumer-record)
          record-value (.value consumer-record)]
      (info "Record value is " record-value)
      (->> (str/upper-case record-value)
           (repeat 5)
           (into [] (map #(ProducerRecord. topic record-key %)))))))


(comment

  (def actor-system (->actor-system "test-actor-system"))

  (def consumer-settings (->consumer-settings actor-system "lalalalala-boom" (StringDeserializer.) (StringDeserializer.) "earliest"))

  (def committable-source (->committable-source consumer-settings "testing_stuff"))

  (def map-asyncly (map-async committable-source 5 (fn [^ConsumerRecord kafka-record]
                                                     (println "The key is " (.key kafka-record))
                                                     (println "The value is " (.value kafka-record))
                                                     (println "The timestamp is " (.timestamp kafka-record))
                                                     (println "The offset is " (.offset kafka-record)))))

  (def producer-settings (->producer-settings actor-system (StringSerializer.) (StringSerializer.) {}))

  (def producer-records-fn (consumer-record->producer-records-fn "testing_stuff"))

  (def mapSync1 (map-sync committable-source (consumer-record->producer-records-envelope-fn producer-records-fn)))

  (def control-reference-atom (atom nil))

  (def restart-source (->restart-source (->restart-settings 60 300 0.2)
                                        committable-source
                                        control-reference-atom
                                        (via-flow mapSync1)
                                        (via-flow (->producer-flexiflow producer-settings))
                                        (map-sync-with-passthrough)
                                        (to-mat (->committer-sink (->committer-settings actor-system 5)))))

  (def via (via-flow mapSync1 (->producer-flexiflow producer-settings)))

  (def mapSync2 (map-sync-with-passthrough via))

  #_(def mat (to-mat map-asyncly (->committer-sink (->committer-settings actor-system 5))))

  (def mat (to-mat mapSync2 (->committer-sink (->committer-settings actor-system 5))))

  (def runnuner (run mat actor-system))

  (def shut-result (shutdown runnuner))

  #_(def restart-soource (-> (RestartSource/onFailuresWithBackoff
                               (->restart-settings 60 300 0.2)
                               (reify
                                 Creator
                                 (create [_]
                                   (-> committable-source
                                       (.mapMaterializedValue (reify Function
                                                                (apply [_ consumer-control]
                                                                  ;; This bit is just to simulate kafka falure
                                                                  (swap! control-reference-atom #(identity %2) consumer-control)
                                                                  consumer-control)))
                                       (.via (.mapAsync (Flow/create) 5 (->async-mapping-function (fn [^ConsumerRecord kafka-record]
                                                                                                    (swap! restart-count inc)
                                                                                                    (throw (Exception. "test hai bro"))
                                                                                                    (println "The key is " (.key kafka-record))
                                                                                                    (println "The value is " (.value kafka-record))
                                                                                                    (println "The timestamp is " (.timestamp kafka-record))
                                                                                                    (println "The offset is " (.offset kafka-record))))))
                                       (.via (Committer/flow (->committer-settings actor-system 5)))))))
                             (.runWith (Sink/ignore) actor-system)))
  #_(.drainAndShutdown @control-reference-atom restart-soource (.getDispatcher actor-system))
  )
