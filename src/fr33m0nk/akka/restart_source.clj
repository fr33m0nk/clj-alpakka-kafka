(ns fr33m0nk.akka.restart-source
  (:require
    [fr33m0nk.utils :as utils])
  (:import
    (akka.stream RestartSettings)
    (akka.stream.javadsl RestartSource)
    (java.time Duration)))

(defn restart-settings
  [min-backoff-millis max-backoff-millis random-factor]
  (RestartSettings/create (Duration/ofMillis min-backoff-millis)
                          (Duration/ofMillis max-backoff-millis)
                          (double random-factor)))

(defn ->restart-source-with-backoff
  [^RestartSettings restart-settings source-factory-fn]
  (RestartSource/withBackoff restart-settings (utils/->fn0 source-factory-fn)))

(defn ->restart-source-on-failures-with-backoff
  [^RestartSettings restart-settings source-factory-fn]
  (RestartSource/onFailuresWithBackoff restart-settings (utils/->fn0 source-factory-fn)))
