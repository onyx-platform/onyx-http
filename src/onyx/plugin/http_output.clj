(ns onyx.plugin.http-output
  (:require [clojure.core.async :as async :refer [<!! <! go timeout]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.peer.function :as function]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t :refer [dec-count! inc-count!]]
            [qbits.jet.client.http :as http]
            [taoensso.timbre :refer [debug info error] :as timbre])
  (:import [java.util Random])) 

(defn- process-message [client url args success? ack-fn async-exception-fn retry-params]
  (go
    (try
     (let [rch (http/post client url args)
           response (<! rch)
           result (if (:error response)
                    :failure
                    (let [body (<! (:body response))
                          fetched (assoc response :body body)]
                      (if (success? fetched)
                        (do
                         (ack-fn)
                         :success)
                        :failure)))]
       (when (and (= result :failure) (:allow-retry? retry-params))
         (let [{:keys [retry-attempt base-sleep-ms max-sleep-ms max-total-sleep-ms initial-request-time]} retry-params
               fuzzy-multiplier (min (max (+ 1.0 (* 0.2 (.nextGaussian (Random.)))) 0) 2)
               next-backoff-ms (int (min max-sleep-ms (* base-sleep-ms (Math/pow 2 retry-attempt) fuzzy-multiplier)))
               ;; Cut-off at maximum retry-cutoff-time. 
               ;; This is useful because the segment will be retried by onyx automatically after :onyx/max-pending-time anyway
               retry? (< (+ (System/currentTimeMillis) next-backoff-ms) (+ initial-request-time max-total-sleep-ms))]
           (error "Request failed" {:url url :args args :response response :retry? retry? :retry-attempt retry-attempt :backoff-ms next-backoff-ms})
           (when retry?
             (<! (timeout next-backoff-ms))
             (process-message client url args success? ack-fn async-exception-fn (assoc retry-params :retry-attempt (inc retry-attempt)))))))
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
                         (process-message client (:url message) (:args message) success? ack-fn async-exception-fn {:allow-retry? false})))
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

(defn basic-auth-header [user password]
  (str "Basic " (.encodeToString (java.util.Base64/getEncoder)
                                 (.getBytes (str user ":" password) "UTF-8"))))

(defn split-serializer [coll serializer-fn max-limit]
  (loop [factor (count coll) 
         partitions (map serializer-fn (partition-all factor coll))]
    (if (and (some #(> (count %) max-limit) partitions)
             (> factor 1))
      (recur (/ factor 2)
             (map serializer-fn (partition-all (/ factor 2) coll)))
      (vector factor partitions))))

(defrecord JetWriterBatch [client success? serializer-fn compress-fn url args async-exception-info batch-byte-size retry-params]
  p-ext/Pipeline
  (read-batch [_ event]
    (onyx.peer.function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger]
        :as event}]
    (when-not (empty? @async-exception-info)
      (throw (ex-info "HTTP Request failed." @async-exception-info)))
    (let [segments (map :message (mapcat :leaves (:tree results)))
          [factor serialized-partitions] (split-serializer segments serializer-fn batch-byte-size)
          ack-partitions (partition-all factor (:acks results))
          retry (assoc retry-params :initial-request-time (System/currentTimeMillis))]
      (assert (= (count segments) (count (:acks results))))
      (run! (fn [ack] (inc-count! ack)) (:acks results))
      (mapv (fn [serialized acks]
              (let [ack-fn (fn []
                             (run! (fn [ack]
                                     (when (dec-count! ack)
                                       (when-let [site (peer-site peer-replica-view (:completion-id ack))]
                                         (extensions/internal-ack-segment messenger site ack)))) 
                                   acks))
                    async-exception-fn (fn [data] (reset! async-exception-info data))]
                (process-message client url (assoc args :body (compress-fn serialized)) success? ack-fn async-exception-fn retry)))
            serialized-partitions
            ack-partitions)
      {:http/written? true}))

  (seal-resource [_ _]
    (.destroy client)
    {}))

(defn add-authorization [args auth-user-env auth-password-env]
  (let [auth-user (System/getenv auth-user-env)
        auth-password (System/getenv auth-password-env)]
    (assoc-in args [:headers "Authorization"] (basic-auth-header auth-user auth-password))))

(defn batch-output [{:keys [onyx.core/task-map] :as pipeline-data}]
  (let [client (http/client)
        {:keys [http-output/url http-output/args http-output/auth-user-env http-output/auth-password-env]} task-map
        batch-byte-size (or (:http-output/batch-byte-size task-map) Integer/MAX_VALUE)
        _ (when (:onyx/fn task-map)
            (throw (Exception. ":onyx/fn cannot currently be supplied to this plugin batching by byte size.")))
        args (cond (and auth-user-env auth-password-env)
                   (add-authorization args auth-user-env auth-password-env)
                   (or (and auth-user-env (not auth-password-env)) 
                       (and (not auth-user-env) auth-password-env))
                   (throw (ex-info "Both auth-user-env and auth-password-env must be supplied." 
                                   {:http-output/auth-password-env auth-password-env
                                    :http-output/auth-user-env auth-user-env}))
                   :else
                   args)
        serializer-fn (kw->fn (:http-output/serializer-fn task-map))
        compress-fn (kw->fn (:http-output/compress-fn task-map))
        success? (kw->fn (or (:http-output/success-fn task-map) ::success?-default))
        async-exception-info (atom {})
        retry-params (if-let [retry-params (:http-output/retry-params task-map)]
                       (assoc retry-params :allow-retry? true :retry-attempt 0)
                       {:allow-retry? false})]
    (->JetWriterBatch client success? serializer-fn compress-fn url args async-exception-info batch-byte-size retry-params)))
