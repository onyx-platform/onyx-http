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

(defn- process-message [client url args success? ack-fn async-exception-fn]
  (go
    (try
      (let [rch      (http/post client url args)
            response (<! rch)]
        (if (:error response)
          (error "Request failed" {:url url :args args :response response})
          (let [body (<! (:body response))
                fetched (assoc response :body body)]
            (if (success? fetched)
              (ack-fn)
              (error "Request" {:url url :args args :response fetched})))))
      (catch Exception e
        (async-exception-fn {:url url :args args :exception e})))))

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
                       (let [message (:message leaf)] 
                         (process-message client (:url message) (:args message) 
                                          success? ack-fn async-exception-fn)))
                 (:leaves result))))
        (map list (:tree results) (:acks results))))
    {:http/written? true})

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

(defrecord JetWriterBatch [client success? serializer-fn url args async-exception-info]
  p-ext/Pipeline
  (read-batch [_ event]
    (onyx.peer.function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger]
        :as event}]
    (let [segments (map :message (mapcat :leaves (:tree results)))]
      (when-not (empty? @async-exception-info)
        (throw (ex-info "HTTP Request failed." @async-exception-info)))
      (when-not (empty? segments)
        (let [body (serializer-fn segments)]
          (run! (fn [ack] (inc-count! ack)) (:acks results))
          (let [ack-fn (fn []
                         (run! (fn [ack]
                                 (when (dec-count! ack)
                                   (when-let [site (peer-site peer-replica-view (:completion-id ack))]
                                     (extensions/internal-ack-segment messenger site ack)))) 
                               (:acks results)))
                async-exception-fn (fn [data] (reset! async-exception-info data))]
            (process-message client url (assoc args :body body) success? ack-fn async-exception-fn))
          {:http/written? true}))))

  (seal-resource [_ _]
    (.destroy client)
    {}))

(defn batch-output [{:keys [onyx.core/task-map] :as pipeline-data}]
  (let [client (http/client)
        url (:http-output/url task-map)
        args (:http-output/args task-map)
        serializer-fn (kw->fn (:http-output/serializer-fn task-map))
        success? (kw->fn (or (:http-output/success-fn task-map) ::success?-default))
        async-exception-info (atom {})]
   (->JetWriterBatch client success? serializer-fn url args async-exception-info)))
