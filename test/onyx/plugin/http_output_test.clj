(ns onyx.plugin.http-output-test
  (:require [clojure.core.async :as a :refer [go chan >! >!! <!! close!]]
            [clojure.test :refer [deftest is]]
            [taoensso.timbre :as log]
            [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]

            [onyx.api]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.http-output])
  (:import [org.apache.log4j Logger Level]))

(-> (Logger/getLogger "io.netty") (.setLevel Level/INFO))


(def messages
  [{:method :post :url "http://localhost:41300/" :args {:body "a=1" :as :json}}
   {:method :post :url "http://localhost:41300/" :args {:body "b=2" :as :json}}
   {:method :post :url "http://localhost:41300/" :args {:body "c=3" :as :json}}])

(def in-buffer (atom nil))
(def in-chan (atom nil))
(def out-chan (atom nil))
(def req-count (atom nil))
(def server (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan
   :core.async/buffer in-buffer})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def id (java.util.UUID/randomUUID))
(def zk-addr "127.0.0.1:2188")

(def env-config
  {:zookeeper/address zk-addr
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/tenancy-id id})

(def peer-config
  {:zookeeper/address zk-addr
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :onyx/tenancy-id id})

(defn success? [{:keys [status body] :as response}]
  (and
    (< status 400)
    (:success body)))

(defn post-process [message response]
  (>!! @out-chan (-> message :args :body)))

(defn async-handler [request]
  (let [id      (bs/to-string (:body request))
        attempt (get @req-count id 0)]
    (println "REQCOUNT" @req-count)

    (cond
      (and (= id "b=2") (< attempt 2))
      (do
        (swap! req-count update id (fnil inc 0))
        {:body   "retry"
         :status 500})

      (= id "c=3")
      {:body   "fail"
       :status 400}

      :else
      {:body    (json/generate-string {:success true})
       :headers {"Content-Type" "application/json"}
       :status  200})))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size 10
    :onyx/batch-timeout 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :do-requests
    :onyx/plugin :onyx.plugin.http-output/output
    :onyx/type :output
    :http-output/success-fn ::success?
    :http-output/post-process-fn ::post-process
    :http-output/retry-params {:base-sleep-ms 100
                               :max-sleep-ms 500
                               :max-total-sleep-ms 1000}
    :onyx/n-peers 1
    :onyx/medium :http
    :onyx/batch-size 10
    :onyx/batch-timeout 1
    :onyx/doc "Sends http POST requests somewhere"}])

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}])

(def workflow
  [[:in :do-requests]])

(def job-conf
  {:catalog catalog
   :workflow workflow
   :lifecycles lifecycles
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest writer-plugin-test
  (when @server
    (.close @server))
  (reset! in-buffer {})
  (reset! in-chan (chan (count messages)))
  (reset! out-chan (chan (count messages)))
  (reset! req-count {})

  (with-test-env [env [2 env-config peer-config]]
    (let [_ (reset! server (http/start-server async-handler {:port 41300}))
          _ (Thread/sleep 500)
          job (onyx.api/submit-job peer-config job-conf)]

      (>!! @in-chan (nth messages 0))
      (>!! @in-chan (nth messages 1))
      (Thread/sleep 20000)
      (>!! @in-chan (nth messages 2))
      (close! @in-chan)

      (let [exc (try
                  (onyx.test-helper/feedback-exception! peer-config (:job-id job))
                  (catch Exception e
                    (ex-data e)))]
        (is (= (select-keys exc [:method :url :args])
              {:method :post :url "http://localhost:41300/" :args {:body "c=3" :as :json}})))

      (onyx.api/await-job-completion peer-config (:job-id job))
      (.close @server)

      (let [results (take-segments! @out-chan 100)]
        (is (= (set results)
              #{"a=1" "b=2"}))))))
