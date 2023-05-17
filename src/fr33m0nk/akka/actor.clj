(ns fr33m0nk.akka.actor
  (:import (akka.actor ActorSystem BootstrapSetup)
           (akka.actor.setup ActorSystemSetup)
           (com.typesafe.config Config)
           (java.util.concurrent CompletionStage)
           (scala.concurrent ExecutionContext)))

(defn ->actor-system
  ([]
   (ActorSystem/create))
  ([name]
   (ActorSystem/create ^String name))
  ([name config-or-bootstrap-setup-or-actor-system-setup]
   (condp instance? config-or-bootstrap-setup-or-actor-system-setup)
   Config (ActorSystem/create ^String name ^Config config-or-bootstrap-setup-or-actor-system-setup)
   BootstrapSetup (ActorSystem/create ^String name ^BootstrapSetup config-or-bootstrap-setup-or-actor-system-setup)
   ActorSystemSetup (ActorSystem/create ^String name ^ActorSystemSetup config-or-bootstrap-setup-or-actor-system-setup))
  ([name ^Config config ^ClassLoader class-loader]
   (ActorSystem/create ^String name ^Config config class-loader))
  ([name ^Config config ^ClassLoader class-loader ^ExecutionContext default-execution-context]
   (ActorSystem/create ^String name ^Config config class-loader default-execution-context)))

(defn get-dispatcher
  [^ActorSystem actor-system]
  (.getDispatcher actor-system))

(defn register-on-termination
  [^ActorSystem actor-system ^Runnable f]
  (.registerOnTermination actor-system f))

(defn terminate
  ^CompletionStage
  [^ActorSystem actor-system]
  (.terminate actor-system)
  (.getWhenTerminated actor-system))
