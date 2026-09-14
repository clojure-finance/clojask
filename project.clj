(defproject com.github.clojure-finance/clojask "2.0.5"
  :description "Data analysis and manipulation library with parallel computing for larger-than-memory datasets"
  :url "https://github.com/clojure-finance/clojask"
  :license {:name "MIT"
            :url "https://github.com/clojure-finance/clojask/blob/1.x.x/LICENSE"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                ;;  [org.clojure/math.numeric-tower "0.0.4"]
                 [org.clojure/data.csv "1.0.0"]
                 [com.github.clojure-finance/onyx "0.16.0"]
                 [com.taoensso/timbre "5.2.1"]
                ;;  [techascent/tech.ml.dataset "5.17" :exclusions [[ch.qos.logback/logback-classic][org.slf4j/slf4j-api]]]
                 [com.google.code.externalsortinginjava/externalsortinginjava "0.6.0"]
                 [com.github.clojure-finance/clojask-io "1.0.6"]
                 [com.github.clojure-finance/clojure-heap "1.0.3"]]
  ;; lein deploy clojars, credentials from CLOJARS_USERNAME / CLOJARS_PASSWORD
  :deploy-repositories [["clojars" {:url "https://clojars.org/repo"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases false}]]
  :repl-options {:timeout 180000}
  :source-paths      ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :javac-options ["--release" "17"]
  ;; Onyx's Aeron messaging (via Agrona) reads jdk.internal.misc.Unsafe.
  ;; lz4 (via Onyx's nippy serialization) calls System.load; JDK 24+ warns without
  ;; native access enabled.
  :jvm-opts ["-XX:+UseG1GC" "-server" "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
             "--enable-native-access=ALL-UNNAMED"]
  :test-paths        ["test/clojask"]
  ;:java-test-paths   ["test/java"]
  ;;:injections [(.. System (setProperty "clojure.core.async.pool-size" "8"))]
  )
