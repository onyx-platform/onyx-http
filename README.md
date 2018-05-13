## onyx-http

Onyx plugin for http.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-http "0.13.0.0-SNAPSHOT"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.http-output])
```

#### Functions

##### http-output

Catalog entry:

```clojure
{:onyx/name :entry-name
 :onyx/plugin :onyx.plugin.http-output/output
 :onyx/type :output
 :onyx/medium :http
 :http-output/success-fn :my.namespace/success?
 :http-output/retry-params {:base-sleep-ms 200
                            :max-sleep-ms 30000
                            :max-total-sleep-ms 3600000}
 :onyx/batch-size batch-size
 :onyx/doc "POST segments to http endpoint"}
```

Segments coming in to your http task are expected in a form such as:
```clojure
{:url "http://localhost:41300/" :args {:body "a=1" :as :json}
```

#### Attributes

|key                            | type      | description
|-------------------------------|-----------|------------
|`:http-output/success-fn`      | `keyword` | Function, which accepts response as an argument, should return boolean to indicate if the response was successful.
|`:http-output/post-process-fn` | `keyword` | Function, accepts `:onyx.core/params`, message and response (in that order) to perform any side-effectful action
|`:http-output/retry-params`    | `map`     | Indicates if request should be retried if it's not successful and parameters of retries. Map of three keys: `:base-sleep-ms` is a delay for a first retry, `:max-sleep-ms` is a maximum retry delay, and `:max-total-sleep-ms` is amount of time when request should be not retried anymore.

#### Acknowledgements

Many thanks to [Vsevolod Solovyov](https://github.com/vsolovyov) for contributing this plugin to Onyx Platform.

#### Contributing

Pull requests into the master branch are welcomed.
