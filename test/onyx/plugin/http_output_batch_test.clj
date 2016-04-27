(ns onyx.plugin.http-output-batch-test
  (:require [clojure.core.async :refer [go chan >! >!! <!! close!]]
            [clojure.test :refer [deftest is]]
            [taoensso.timbre :refer [info]]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.http-output]
            [qbits.jet.server]
            [onyx.api])
  (:import [java.util.zip GZIPOutputStream GZIPInputStream]
           [java.io StringWriter]
           [java.io BufferedReader InputStreamReader ByteArrayInputStream ByteArrayOutputStream]))

(defn str->gzip [s encoding]
  (let [baos (ByteArrayOutputStream.)
        gzos (GZIPOutputStream. baos)]
    (.write gzos (.getBytes s encoding))
    (.close gzos)
    (.toByteArray baos)))

(def str->gzip-utf8 #(str->gzip (str (vec %)) "UTF-8"))

(defn gzip->str [input-stream]
  (loop [reader (BufferedReader. (InputStreamReader. (GZIPInputStream. input-stream)))
         s (StringWriter.)]
    (if-let [line (.readLine reader)]
      (do (.write s line)
          (recur reader s))
      (.toString s))))

(def messages
  [{:a 1 :b 2}
   {:a 3 :b 3}
   :done])

(def in-chan (chan (count messages)))
(def out-chan (chan (count messages)))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

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

(defn async-handler [request]
  (let [ch (chan)]
    (go
      (>! out-chan {:body (gzip->str (:body request))})
      (>! ch
        {:body "{\"success\": true}"
         :headers {"Content-Type" "json"}
         :status 200}))
   ch))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size 10
    :onyx/batch-timeout 50
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :do-requests
    :onyx/plugin :onyx.plugin.http-output/batch-output
    :onyx/type :output
    :http-output/success-fn ::success?
    :http-output/url "http://localhost:41300/" 
    ;:http-output/auth-user-env "hi"
    ;:http-output/auth-password-env "hey"
    :http-output/args {:as :json
                       :headers {"content-type" "application/json"
                                 "content-encoding" "gzip"}}
    :http-output/serializer-fn ::str->gzip-utf8
    :onyx/n-peers 1
    :onyx/medium :http
    :onyx/batch-size 10
    :onyx/batch-timeout 50
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
  (with-test-env [env [2 env-config peer-config]]
    (let [job (onyx.api/submit-job peer-config job-conf)
          _ (info "Starting Jetty server")
          server (qbits.jet.server/run-jetty {:ring-handler async-handler :port 41300
                                              :join? false})
          _ (info "Started Jetty server")
          _ (doseq [v messages]
              (>!! in-chan v))
          _ (close! in-chan)
          _ (info "Awaiting job completion")
          _ (onyx.api/await-job-completion peer-config (:job-id job))
          _ (info "Job completed")
          _ (>!! out-chan :done) ;; for take-segments!
          results (take-segments! out-chan)
          _ (info "Stopping Jetty server")
          _ (.stop server)
          _ (.destroy server)
          _ (info "Stopped Jetty server")
          ]
      (is
        (= {:body "[{:a 1, :b 2} {:a 3, :b 3}]"} (first results))))))
