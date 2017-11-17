(defproject org.onyxplatform/onyx-http "0.12.0.0-SNAPSHOT"
  :description "Onyx plugin for HTTP"
  :url "https://github.com/onyx-platform/onyx-http"
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.12.0-20171117_023502-gbb146ad"]
                 [aleph "0.4.4"]
                 [io.netty/netty-all "4.1.12.Final"]]
  :profiles {:dev {:dependencies [[cheshire "5.8.0"]]
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
