(ns onyx.plugin.http-output
  (:require [clojure.core.async :as async :refer [<!! <! go]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.peer.function :as function]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t :refer [dec-count! inc-count!]]
            [qbits.jet.client.http :as http]
            [taoensso.timbre :refer [debug info error] :as timbre]))

(defn- process-message [client success? message ack-fn async-exception-fn]
  (go
    (try
      (let [rch      (http/post client (:url message) (:args message))
            response (<! rch)]
        (if (:error response)
          (error "Request failed" {:request message :response response})
          (let [body (<! (:body response))
                fetched (assoc response :body body)]
            (if (success? fetched)
              (ack-fn)
              (error "Request" {:request message :response fetched})))))
      (catch Exception e
        (async-exception-fn {:request message :exception e})))))

(defrecord JetWriter [client success? async-exception-info]
  p-ext/Pipeline
  (read-batch [_ event]
    (onyx.peer.function/read-batch event))

  ;; written after onyx-bookkeeper plugin
  (write-batch
    [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger]
        :as event}]
    (when-not (empty? @async-exception-info)
      (throw (ex-info "HTTP Request failed." @async-exception-info)))
    (doall
      (map (fn [[result ack]]
             (run! (fn [_] (inc-count! ack)) (:leaves result))
             (let [ack-fn (fn []
                            (when (dec-count! ack)
                              (when-let [site (peer-site peer-replica-view (:completion-id ack))]
                                (extensions/internal-ack-segment messenger site ack))))
                   async-exception-fn (fn [data] (reset! async-exception-info data))]
               (run! (fn [leaf]
                       (process-message client success? (:message leaf)
                         ack-fn async-exception-fn))
                 (:leaves result))))
        (map list (:tree results) (:acks results))))
    {:onyx.core/written? true})

  (seal-resource [_ _]
    (.destroy client)
    {}))

(defn success?-default [{:keys [status]}]
  (< status 500))

(defn output [{:keys [onyx.core/task-map] :as pipeline-data}]
  (let [client (http/client)
        success? (kw->fn (or (:http-output/success-fn task-map) ::success?-default))
        async-exception-info (atom {})]
   (->JetWriter client success? async-exception-info)))
