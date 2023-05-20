(ns fr33m0nk.akka.restart-source
  (:require
    [fr33m0nk.utils :as utils])
  (:import
    (akka.stream RestartSettings)
    (akka.stream.javadsl RestartSource)
    (java.time Duration)))

(defn restart-settings
  "Creates restart settings for restart source"
  [min-backoff-millis max-backoff-millis random-factor
   {:keys [max-restart-count max-restarts-within-millis restart-when-exception-handler]}]
  (cond-> (RestartSettings/create (Duration/ofMillis min-backoff-millis)
                                  (Duration/ofMillis max-backoff-millis)
                                  (double random-factor))
    (and max-restart-count max-restarts-within-millis) (.withMaxRestarts (int max-restart-count) (Duration/ofMillis max-restarts-within-millis))
    restart-when-exception-handler (.withRestartOn (utils/->fn1 restart-when-exception-handler))))

(defn ->restart-source-with-backoff
  [^RestartSettings restart-settings source-factory-fn]
  (RestartSource/withBackoff restart-settings (utils/->fn0 source-factory-fn)))

(defn ->restart-source-on-failures-with-backoff
  [^RestartSettings restart-settings source-factory-fn]
  (RestartSource/onFailuresWithBackoff restart-settings (utils/->fn0 source-factory-fn)))
