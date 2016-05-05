## onyx-http

Onyx plugin for http.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-http "0.9.6.1-SNAPSHOT"]
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
 :onyx/batch-size batch-size
 :onyx/doc "POST segments to http endpoint"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.http/lifecycle-calls}]
```

Segments coming in to your http task are expected in a form such as:
```clojure
{:url "http://localhost:41300/" :args {:body "a=1" :as :json}
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:http-output/success-fn`     | `keyword` | Accepts response as argument, should return boolean to indicate if the response was successful. Request will be retried in case it wasn't a success.


##### Batch http-output

The batch http-output plugin will combine a batch's segments into a single http output request.

As the segments can no longer be used for request parameters, these are instead supplied via the task-map. 
A serializer-fn is also provided in case gzip requests need to be made, although :clojure.core/str will generally be used.

```clojure
{:onyx/name :your-task-name
 :onyx/plugin :onyx.plugin.http-output/batch-output
 :onyx/type :output
 :http-output/success-fn ::success?
 :http-output/url "http://localhost:41300/" 
 :http-output/args {:as :json
                    :headers {"content-type" "application/json"
                              "content-encoding" "gzip"}}
 :http-output/serializer-fn ::str->gzip
 ;; Optional basic authentication settings
 ;:http-output/auth-password-env "HTTP_USER_ENV"
 ;:http-output/auth-user-env "HTTP_PASS_ENV"
 :onyx/n-peers 1
 :onyx/medium :http
 :onyx/batch-size 10
 :onyx/batch-timeout 50
 :onyx/doc "Sends http POST requests somewhere"}
```
Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.http/lifecycle-calls}]
```
#### Attributes

|key                             | type      | description
|--------------------------------|-----------|------------
|`:http-output/success-fn`       | `keyword` | Accepts response as argument, should return boolean to indicate if the response was successful. Request will be retried in case it wasn't a success.
|`:http-output/args`             | `map`     | http-request args, see http://mpenet.github.io/jet/qbits.jet.client.http.html#var-request
|`:http-output/url`              | `string`  | The url that the request will be made to
|`:http-output/serializer-fn`    | `keyword` | Namespaced keyword pointing to a serializer fn that will be used to encode the body of the request.
|`:http-output/auth-user-env`    | `string`  | String reference to an environment variable holding an username for basic authentication
|`:http-output/auth-password-env`| `string`  | String reference to an environment variable holding a password for basic authentication

#### Acknowledgements

Many thanks to [Vsevolod Solovyov](Vsevolod Solovyo://github.com/vsolovyov) for contributing this plugin to Onyx Platform.


#### Contributing

Pull requests into the master branch are welcomed.
