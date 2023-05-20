# Introduction to fr33m0nk/clj-alpakka-kafka

## Using Alpakka Kafka stream with a sink 

#### This is Clojure adaptation of example from [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#offset-storage-in-kafka-committing)

1. First we will import required namespaces:

```clojure
(require 
 '[fr33m0nk.akka.actor :as actor]
 '[fr33m0nk.akka.stream :as s]
 '[fr33m0nk.alpakka-kafka.committer :as committer]
 '[fr33m0nk.alpakka-kafka.consumer :as consumer]
 '[fr33m0nk.utils :as utils])
```
2. Now we will create a stream topology
  - This topology consumes messages
  - `s/map-async` executes mapping function with 2 messages being processed in parallel
    - Processing of `s/map-async` may not be in order, however output of `s/map-async` is always in order
    - If processing order is needed, use `s/map`

  - Then we commit offsets to Kafka via `s/to-mat` and `committer/sink`
  - Finally, we run the stream with our actor-system using `s/run`
```clojure
(defn test-stream
  [actor-system consumer-settings committer-settings consumer-topics]
  (-> (consumer/->committable-source consumer-settings consumer-topics)
      (s/map-async 2
                   (fn [message]
                     (let [key (consumer/key message)
                           value (consumer/value message)]
                       ;; Do business processing    
                       (println "Key is " key)
                       (println "Value is " value))
                       ;; Return message
                     message)
                     ;; then-fn returns committable offset for offset committing
                   (fn [message]
                     (consumer/committable-offset message)))
      (s/to-mat (committer/sink committer-settings) consumer/create-draining-control)
      (s/run actor-system)))
```
3. Lets now run the stream and see it in action
```clojure
;; Create actor system
(def actor-system (actor/->actor-system "test-actor-system"))

;; Create committer settings
(def committer-settings (committer/committer-settings actor-system {:batch-size 1}))

;; Create consumer-settings
(def consumer-settings (consumer/->consumer-settings actor-system
                                                       {:group-id "a-test-consumer"
                                                        :bootstrap-servers "localhost:9092"
                                                        :key-deserializer (StringDeserializer.)
                                                        :value-deserializer (StringDeserializer.)}))

;; holding on to consumer-control to shutdown streams
(def consumer-control (test-stream actor-system consumer-settings committer-settings ["testing_stuff"]))
```
4. Stream in action 😃
<img width="1520" alt="image" src="https://github.com/fr33m0nk/clj-alpakka-kafka/assets/43627165/58e6e972-9aa3-41a4-bb43-c3c55e5d2a81">
5. Let's shutdown the stream now

```clojure
;; shutdown streams using consumer-control var
@(consumer/drain-and-shutdown consumer-control
                             (CompletableFuture/supplyAsync
                               (utils/->fn0 (fn [] ::done)))
                             (actor/get-dispatcher actor-system))
```
6. Let's shutdown our actor-system as well

```clojure
@(actor/terminate actor-system)
```

## Using Alpakka Kafka stream with a Kafka Producer

#### This is Clojure adaptation of example from [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#connecting-producer-and-consumer)

1. Let's require following namespace
```clojure
(require '[fr33m0nk.alpakka-kafka.producer :as producer])
```
2. We will create a new stream topology
  - This topology consumes messages
  - `s/map-async` executes mapping function with 2 messages being processed in parallel
  - Then we will publish messages to another topic and commit offsets to Kafka via `s/to-mat` and `producer/committable-sink`
  - Finally, we run the stream with our actor-system using `s/run`
```clojure
(defn test-stream-with-producer
  [actor-system consumer-settings committer-settings producer-settings consumer-topics producer-topic]
  (-> (consumer/->committable-source consumer-settings consumer-topics)
      (s/map-async 2
                   (fn [message]
                     (let [_key (consumer/key message)      ;; Don't care as it is null
                           value (consumer/value message)
                           committable-offset (consumer/committable-offset message)
                           message-to-publish (producer/->producer-record producer-topic (str/upper-case value))]
                       (producer/single-producer-message-envelope committable-offset message-to-publish))))
      (s/to-mat (producer/committable-sink producer-settings committer-settings) consumer/create-draining-control)
      (s/run actor-system)))
```
3. Let's create required dependencies
```clojure
(def actor-system (actor/->actor-system "test-actor-system"))

(def committer-settings (committer/committer-settings actor-system {:batch-size 1}))

(def consumer-settings (consumer/consumer-settings actor-system
                                                   {:group-id "a-test-consumer"
                                                    :bootstrap-servers "localhost:9092"
                                                    :key-deserializer (StringDeserializer.)
                                                    :value-deserializer (StringDeserializer.)}))

(def producer-settings (producer/producer-settings actor-system {:bootstrap-servers "localhost:9092"
                                                                 :key-serializer (StringSerializer.)
                                                                 :value-serializer (StringSerializer.)}))
```
4. Let's run the stream and see it in action
```clojure
(def consumer-control (test-stream-with-producer actor-system consumer-settings committer-settings producer-settings ["testing_stuff"] "output-topic"))
```
5. Streams in action 😃
<img width="1724" alt="image" src="https://github.com/fr33m0nk/clj-alpakka-kafka/assets/43627165/3176e50d-f5c3-453b-830f-c97f2aecfdaf">

6. Let's shutdown the stream now

```clojure
;; shutdown streams using consumer-control var
@(consumer/drain-and-shutdown consumer-control
                             (CompletableFuture/supplyAsync
                               (utils/->fn0 (fn [] ::done)))
                             (actor/get-dispatcher actor-system))
```
7. Let's shutdown our actor-system as well

```clojure
@(actor/terminate actor-system)
```

## Using Alpakka Kafka stream with a Kafka Producer for producing multiple messages

#### This is Clojure adaptation of example from [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#connecting-producer-and-consumer)

1. We will create a new stream topology
- This topology consumes messages
- `s/map-async` executes mapping function with 2 messages being processed in parallel and produces multiple producer records
- These producer-records are wrapped in `producer/multi-producer-message-envelope` which ensures offset commits only happen when all the producer-records are published
- Then we will publish messages to another topic and commit offsets to Kafka via `s/to-mat` and `producer/committable-sink`
- Finally, we run the stream with our actor-system using `s/run`
```clojure
(defn test-stream-with-producing-multiple-messages
  [actor-system consumer-settings committer-settings producer-settings consumer-topics producer-topic]
  (-> (consumer/->committable-source consumer-settings consumer-topics)
      (s/map-async 2
                   (fn [message]
                     (let [_key (consumer/key message)      ;; Don't care as it is null
                           value (consumer/value message)
                           committable-offset (consumer/committable-offset message)
                           messages-to-publish (->> (repeat 5 value)
                                                    (mapv #(producer/->producer-record producer-topic (str/upper-case %))))]
                       (producer/multi-producer-message-envelope committable-offset messages-to-publish))))
      (s/to-mat (producer/committable-sink producer-settings committer-settings) consumer/create-draining-control)
      (s/run actor-system)))
```
2. Let's create required dependencies
```clojure
(def actor-system (actor/->actor-system "test-actor-system"))

(def committer-settings (committer/committer-settings actor-system {:batch-size 1}))

(def consumer-settings (consumer/consumer-settings actor-system
                                                   {:group-id "a-test-consumer"
                                                    :bootstrap-servers "localhost:9092"
                                                    :key-deserializer (StringDeserializer.)
                                                    :value-deserializer (StringDeserializer.)}))

(def producer-settings (producer/producer-settings actor-system {:bootstrap-servers "localhost:9092"
                                                                 :key-serializer (StringSerializer.)
                                                                 :value-serializer (StringSerializer.)}))
```
3. Let's run the stream and see it in action
```clojure
(def consumer-control (test-stream-with-producing-multiple-messages actor-system consumer-settings committer-settings producer-settings ["testing_stuff"] "output-topic"))
```
4. Streams in action 😃
<img width="1571" alt="image" src="https://github.com/fr33m0nk/clj-alpakka-kafka/assets/43627165/20ad1533-e50a-4040-96ce-a9694db3b524">
   

5. Let's shutdown the stream now

```clojure
;; shutdown streams using consumer-control var
@(consumer/drain-and-shutdown consumer-control
                             (CompletableFuture/supplyAsync
                               (utils/->fn0 (fn [] ::done)))
                             (actor/get-dispatcher actor-system))
```
6. Let's shutdown our actor-system as well

```clojure
@(actor/terminate actor-system)
```

## Using Alpakka Kafka stream with At-Least-Once Delivery

#### This is Clojure adaptation of example from [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/atleastonce.html#multiple-effects-per-commit)

1. We will create a new stream topology
- This topology consumes messages
- `s/map-async` executes mapping function with 2 messages being processed in parallel and produces multiple producer records
- These producer-records are wrapped in `producer/multi-producer-message-envelope` which ensures offset commits only happen when all the producer-records are published
- Then we will publish messages to another topic and commit offsets to Kafka via `s/to-mat` and `producer/committable-sink`
- Finally, we run the stream with our actor-system using `s/run`
```clojure
(defn test-stream-producing-multiple-messages-with-at-least-once-semantics
  [actor-system consumer-settings committer-settings producer-settings consumer-topics producer-topic]
  (-> (consumer/->committable-source consumer-settings consumer-topics)
      (s/map-async 2
                   (fn [message]
                     (let [_key (consumer/key message)      ;; Don't care as it is null
                           value (consumer/value message)
                           committable-offset (consumer/committable-offset message)
                           messages-to-publish (->> (repeat 3 value)
                                                    (mapv #(producer/->producer-record producer-topic (str/upper-case %))))]
                       (producer/multi-producer-message-envelope committable-offset messages-to-publish))))
      (s/via (producer/flexi-flow producer-settings))
      (s/map producer/producer-message-passthrough)
      (s/to-mat (committer/sink committer-settings) consumer/create-draining-control)
      (s/run actor-system)))
```
2. Let's create required dependencies
```clojure
(def actor-system (actor/->actor-system "test-actor-system"))

(def committer-settings (committer/committer-settings actor-system {:batch-size 1}))

(def consumer-settings (consumer/consumer-settings actor-system
                                                   {:group-id "a-test-consumer"
                                                    :bootstrap-servers "localhost:9092"
                                                    :key-deserializer (StringDeserializer.)
                                                    :value-deserializer (StringDeserializer.)}))

(def producer-settings (producer/producer-settings actor-system {:bootstrap-servers "localhost:9092"
                                                                 :key-serializer (StringSerializer.)
                                                                 :value-serializer (StringSerializer.)}))
```
3. Let's run the stream and see it in action
```clojure
(def consumer-control (test-stream-producing-multiple-messages-with-at-least-once-semantics actor-system consumer-settings committer-settings producer-settings ["testing_stuff"] "output-topic"))
```
4. Streams in action 😃
<img width="2849" alt="image" src="https://github.com/fr33m0nk/clj-alpakka-kafka/assets/43627165/510fed9f-efb4-4ec4-a031-0b24c51c5462">

5. Let's shutdown the stream now

```clojure
;; shutdown streams using consumer-control var
@(consumer/drain-and-shutdown consumer-control
                             (CompletableFuture/supplyAsync
                               (utils/->fn0 (fn [] ::done)))
                             (actor/get-dispatcher actor-system))
```
6. Let's shutdown our actor-system as well

```clojure
@(actor/terminate actor-system)
```

## Using Alpakka Kafka stream with Error handling

#### This is Clojure adaptation of example from [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/errorhandling.html#restarting-the-stream-with-a-backoff-stage)

1. We will add a restart-counter to keep restart-source's action count and require restart-source namespace
```clojure
(def restart-counter (atom 0))

(require '[fr33m0nk.akka.restart-source :as restart])
```
2. Due to limitation with the design of restart-source, consumer-control is not directly accessible. In order to gain access, we will use an atom
  - Reference
    - https://doc.akka.io/docs/alpakka-kafka/current/errorhandling.html#restarting-the-stream-with-a-backoff-stage
    - https://github.com/akka/alpakka-kafka/issues/1291
```clojure
(def consumer-control-reference (atom nil))
```
3. We will create a new stream topology such that
- This topology fails 3 times and throws exception. After throwing 3 times it proceeds with following
- This topology consumes messages
- `s/map` executes mapping function with 2 messages being processed in parallel and produces multiple producer records
- These producer-records are wrapped in `producer/multi-producer-message-envelope` which ensures offset commits only happen when all the producer-records are published
- Then we will publish messages to another topic and commit offsets to Kafka via `s/to-mat` and `producer/committable-sink`
- Finally, we run the stream with our actor-system using `s/run`
```clojure
(defn test-stream-with-error-handling
  [actor-system restart-settings consumer-settings committer-settings producer-settings consumer-topics producer-topic]
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
                       (when (< @restart-counter 3)
                         (throw (ex-info "Simulating processing failure" {:error-count (swap! restart-counter inc)})))
                       (let [_key (consumer/key message)    ;; Don't care as it is null
                             value (consumer/value message)
                             committable-offset (consumer/committable-offset message)
                             messages-to-publish (->> (repeat 3 value)
                                                      (mapv #(producer/->producer-record producer-topic (str/upper-case %))))]
                         (producer/multi-producer-message-envelope committable-offset messages-to-publish))))
              (s/via (producer/flexi-flow producer-settings))
              (s/map producer/producer-message-passthrough)
              (s/via (committer/flow committer-settings)))))
      (s/run-with s/ignoring-sink actor-system)))
```
4. Let's create required dependencies
```clojure
(def actor-system (actor/->actor-system "test-actor-system"))

(def committer-settings (committer/committer-settings actor-system {:batch-size 1}))

(def consumer-settings (consumer/consumer-settings actor-system
                                                   {:group-id "a-test-consumer"
                                                    :bootstrap-servers "localhost:9092"
                                                    :key-deserializer (StringDeserializer.)
                                                    :value-deserializer (StringDeserializer.)}))

(def producer-settings (producer/producer-settings actor-system {:bootstrap-servers "localhost:9092"
                                                                 :key-serializer (StringSerializer.)
                                                                 :value-serializer (StringSerializer.)}))

(def restart-settings (restart/restart-settings 1000 5000 0.2 {}))
```
5. Let's run the stream and see it in action
```clojure
(def restart-source (test-stream-with-error-handling actor-system
                                                     restart-settings
                                                     consumer-settings
                                                     committer-settings
                                                     producer-settings
                                                     ["testing_stuff"]
                                                     "output-topic"))
```
6. Error handling in action 😃
  - **logs:**
```logcatfilter
[INFO] [05/21/2023 03:25:40.542] [test-actor-system-akka.actor.default-dispatcher-4] [SingleSourceLogic(akka://test-actor-system)] [164b3] Completing
[WARN] [05/21/2023 03:25:40.548] [test-actor-system-akka.actor.default-dispatcher-4] [RestartWithBackoffSource(akka://test-actor-system)] Restarting stream due to failure [1]: clojure.lang.ExceptionInfo: Simulating processing failure {:error-count 1}
clojure.lang.ExceptionInfo: Simulating processing failure {:error-count 1}

[INFO] [05/21/2023 03:25:42.320] [test-actor-system-akka.actor.default-dispatcher-8] [SingleSourceLogic(akka://test-actor-system)] [3955d] Completing
[WARN] [05/21/2023 03:25:42.323] [test-actor-system-akka.actor.default-dispatcher-4] [RestartWithBackoffSource(akka://test-actor-system)] Restarting stream due to failure [2]: clojure.lang.ExceptionInfo: Simulating processing failure {:error-count 2}
clojure.lang.ExceptionInfo: Simulating processing failure {:error-count 2}

[INFO] [05/21/2023 03:25:45.315] [test-actor-system-akka.actor.default-dispatcher-6] [SingleSourceLogic(akka://test-actor-system)] [83500] Completing
[WARN] [05/21/2023 03:25:45.316] [test-actor-system-akka.actor.default-dispatcher-4] [RestartWithBackoffSource(akka://test-actor-system)] Restarting stream due to failure [3]: clojure.lang.ExceptionInfo: Simulating processing failure {:error-count 3}
clojure.lang.ExceptionInfo: Simulating processing failure {:error-count 3}
```
  - **restart counter:**
```clojure
@restart-counter
;;=> 3
```

7. After 3 restarts, state of stream is healthy again 😃
<img width="2850" alt="image" src="https://github.com/fr33m0nk/clj-alpakka-kafka/assets/43627165/09f885a2-ddcc-4c31-88a2-340d6b4a202e">


8. Let's shutdown the stream now

```clojure
;; shutdown streams using consumer-control var
@(consumer/drain-and-shutdown @consumer-control-reference
                              (CompletableFuture/supplyAsync
                                (utils/->fn0 (fn [] ::done)))
                              (actor/get-dispatcher actor-system))
```
9. Let's shutdown our actor-system as well

```clojure
@(actor/terminate actor-system)
```
