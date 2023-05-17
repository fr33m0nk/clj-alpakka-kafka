(ns fr33m0nk.alpakka-kafka.committer
  (:import (akka.actor ActorSystem)
           (akka.kafka CommitDelivery CommitWhen CommitterSettings)
           (akka.kafka.javadsl Committer)
           (java.time Duration)))

(defn committer-settings
  ^CommitterSettings
  [^ActorSystem actor-system & {:keys [batch-size max-interval-in-ms parallelism wait-for-commit-ack commit-when-offset-first-observed]
                                :or {wait-for-commit-ack true
                                     commit-when-offset-first-observed true}
                                :as _options}]
  (cond-> (CommitterSettings/create actor-system)
    batch-size (.withMaxBatch batch-size)
    max-interval-in-ms (.withMaxInterval (Duration/ofMillis max-interval-in-ms))
    parallelism (.withParallelism (int parallelism))
    wait-for-commit-ack (.withDelivery (CommitDelivery/waitForAck))
    (not wait-for-commit-ack) (.withDelivery (CommitDelivery/sendAndForget))
    commit-when-offset-first-observed (.withCommitWhen (CommitWhen/offsetFirstObserved))
    (not commit-when-offset-first-observed) (.withCommitWhen (CommitWhen/nextOffsetObserved))))

(defn sink
  [^CommitterSettings committer-settings]
  (Committer/sink committer-settings))

(defn sink-with-offset-context
  [^CommitterSettings committer-settings]
  (Committer/sinkWithOffsetContext committer-settings))

(defn flow
  [^CommitterSettings committer-settings]
  (Committer/flow committer-settings))

(defn flow-with-offset-context
  [^CommitterSettings committer-settings]
  (Committer/flowWithOffsetContext committer-settings))
