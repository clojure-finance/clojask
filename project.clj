(defproject clojask "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[clojure-csv "2.0.2"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/math.numeric-tower "0.0.4"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.14.5"]
                 [techascent/tech.ml.dataset "5.17" :exclusions [[ch.qos.logback/logback-classic][org.slf4j/slf4j-api]]]
                 [com.google.code.externalsortinginjava/externalsortinginjava "0.6.0"]]
  :repl-options {:init-ns clojask.debug
                 :timeout 60000}
  :plugins [[lein-update-dependency "0.1.2"]]
  :main ^:skip-aot clojask.debug/-main
  :source-paths      ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :test-paths        ["test/clojask"]
  ;:java-test-paths   ["test/java"]
  ;;:injections [(.. System (setProperty "clojure.core.async.pool-size" "8"))]
  )
