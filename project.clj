(defproject com.github.clojure-finance/clojask "1.2.5"
  :description "Data analysis and manipulation library with parallel computing for larger-than-memory datasets"
  :url "https://github.com/clojure-finance/clojask"
  :license {:name "MIT"
            :url "https://github.com/clojure-finance/clojask/blob/1.x.x/LICENSE"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                ;;  [org.clojure/math.numeric-tower "0.0.4"]
                 [org.clojure/data.csv "1.0.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.14.6"]
                ;;  [techascent/tech.ml.dataset "5.17" :exclusions [[ch.qos.logback/logback-classic][org.slf4j/slf4j-api]]]
                 [com.google.code.externalsortinginjava/externalsortinginjava "0.6.0"]
                 [com.github.clojure-finance/clojask-io "1.0.4"]
                 [com.github.clojure-finance/clojure-heap "1.0.3"]]
  :repl-options {:init-ns clojask.debug
                 :timeout 180000}
  :plugins [[lein-update-dependency "0.1.2"]]
  :main ^:skip-aot clojask.debug/-main
  :source-paths      ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :test-paths        ["test/clojask"]
  ;:java-test-paths   ["test/java"]
  ;;:injections [(.. System (setProperty "clojure.core.async.pool-size" "8"))]
  )
