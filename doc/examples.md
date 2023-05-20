# Introduction to fr33m0nk/clj-alpakka-kafka

## Using Alpakka Kafka stream with a sink

1. First we will import required namespaces:

```clojure
(require 
 '[fr33m0nk.akka.actor :as actor]
 '[fr33m0nk.akka.stream :as s]
 '[fr33m0nk.alpakka-kafka.committer :as committer]
 '[fr33m0nk.alpakka-kafka.consumer :as consumer]
 '[fr33m0nk.utils :as utils])
```
2. Now we well create a stream topology
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
                     (let [consumer-record (consumer/consumer-record message)
                           key (.key consumer-record)
                           value (.value consumer-record)]
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
(consumer/drain-and-shutdown consumer-control
                             (CompletableFuture/supplyAsync
                               (utils/->fn0 (fn [] ::done)))
                             (actor/get-dispatcher actor-system))
```
6. Let's shutdown our actor-system as well

```clojure
(actor/terminate actor-system)
```
