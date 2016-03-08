## onyx-http

Onyx plugin for http.

#### Installation

In your project file:

```clojure
[onyx-http "0.9.0.0-alpha9"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.http-output])
```

#### Functions

##### sample-entry

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


#### Acknowledgements

Many thanks to [Vsevolod Solovyov](Vsevolod Solovyo://github.com/vsolovyov) for contributing this plugin to Onyx Platform.


#### Contributing

Pull requests into the master branch are welcomed.
