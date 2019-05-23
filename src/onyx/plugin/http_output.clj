(ns onyx.plugin.http-output
  (:require [onyx.static.util :refer [kw->fn]]
            [onyx.plugin.protocols :as p]
            [taoensso.timbre :as log]
            [aleph.http :as http]
            [manifold.deferred :as d])
  (:import [java.util Random]))


(defn next-backoff [attempt params]
  (when (:allow-retry? params)
    (let [fuzzy-multiplier (min (max (+ 1.0 (* 0.2 (.nextGaussian (Random.)))) 0) 2)
          next-backoff-ms  (int (min (:max-sleep-ms params)
                                  (* (:base-sleep-ms params)
                                    (Math/pow 2 attempt)
                                    fuzzy-multiplier)))]
      (when (< (+ (System/currentTimeMillis) next-backoff-ms)
               (+ (:initial-request-time params) (:max-total-sleep-ms params)))
        next-backoff-ms))))


(defn http-request [method url args success? async-exception-fn]
  (-> (http/request (assoc args
                      :request-method method
                      :url url))

      (d/chain
        (fn [response]
          (if (success? response)
            [true response]
            [false {:method method :url url :args args
                    :response response}])))

      (d/catch Exception
          (fn [e]
            [false {:method method :url url :args args
                    :exception (pr-str e)}]))))


(defn process-message [message success? post-process ack-fn async-exception-fn retry-params run-state]
  "Retry params:
   - allow-retry? - if we will retry or not
   - initial-request-time - time of first request
   - base-sleep-ms - ...
   - max-sleep-ms - ...
   - max-total-sleep-ms - ..."
  (let [{:keys [method url args]
         :or   {method :post}} message]
    (d/loop [attempt 0]
      (log/infof "Making HTTP request: %S %s %.30s attempt %d"
        (name method) url args attempt)
      (d/chain
        (http-request method url args success? async-exception-fn)

        (fn [[is-successful response]]
          (let [next-backoff-ms (next-backoff attempt retry-params)]
            (cond
              is-successful
              (do
                (when post-process
                  (post-process message response))
                (ack-fn))

              next-backoff-ms
              (if @run-state 
                (do
                  (log/debugf "Backing off HTTP request: %S %s %.30s next retry in %d ms"
                              (name method) url args next-backoff-ms)
                  (Thread/sleep next-backoff-ms)
                  (d/recur (inc attempt)))
                (do
                  (log/warnf "Aborting HTTP %s request to %s due to stopped peer" (name method) url)
                  (async-exception-fn response)))

              :else
              (async-exception-fn response))))))))

(defn check-exception! [async-exception-info]
  (when (not-empty @async-exception-info)
    (throw (ex-info "HTTP request failed!" @async-exception-info))))

(deftype HttpOutput [success? post-process retry-params
                     ^:unsynchronized-mutable async-exception-info
                     ^:unsynchronized-mutable in-flight-writes
                     run-state]
  p/Plugin
  (start [this event]
    (reset! run-state true)
    this)

  (stop [this event] 
    (reset! run-state false)
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    (check-exception! async-exception-info)
    (zero? @in-flight-writes))
  (completed? [this]
    (check-exception! async-exception-info)
    (zero? @in-flight-writes))

  p/Checkpointed
  (recover! [this replica-version checkpointed]
    ;; need a whole new atom so async writes from before the recover
    ;; don't alter the counter
    (set! in-flight-writes (atom 0))
    (set! async-exception-info (atom nil))
    this)
  (checkpoint [this])
  (checkpointed! [this epoch])

  p/Output
  (prepare-batch [this event _ _] true)
  (write-batch [this {:keys [onyx.core/write-batch onyx.core/params] :as event} _ _]
    (check-exception! async-exception-info)
    (let [ack-fn             #(swap! in-flight-writes dec)
          async-exception-fn #(reset! async-exception-info %)
          retry              (assoc retry-params :initial-request-time
                               (System/currentTimeMillis))
          post-process       (apply partial post-process params)]
      (run! (fn [message]
              (swap! in-flight-writes inc)
              (process-message message
                success? post-process ack-fn async-exception-fn retry run-state))
            write-batch))
    true))


(defn success?-default [{:keys [status]}]
  (< status 500))


(defn post-process-default [& args]
  nil)


(defn output [{:keys [onyx.core/task-map] :as pipeline-data}]
  (let [success?     (kw->fn (or (:http-output/success-fn task-map)
                                 ::success?-default))
        post-process (kw->fn (or (:http-output/post-process-fn task-map)
                                 ::post-process-default))
        retry-params (:http-output/retry-params task-map)
        retry-params (assoc retry-params :allow-retry? (some? retry-params))]
    (->HttpOutput success? post-process retry-params (atom nil) (atom 0) (atom false))))
