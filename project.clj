(defproject com.github.clojure-finance/clojask "2.1.2"
  :description "Data analysis and manipulation library with parallel computing for larger-than-memory datasets"
  :url "https://github.com/clojure-finance/clojask"
  :license {:name "MIT"
            :url "https://github.com/clojure-finance/clojask/blob/1.x.x/LICENSE"}
  :dependencies [[org.clojure/clojure "1.12.6"]
                ;;  [org.clojure/math.numeric-tower "0.0.4"]
                ;;  [techascent/tech.ml.dataset "5.17" :exclusions [[ch.qos.logback/logback-classic][org.slf4j/slf4j-api]]]
                 [com.google.code.externalsortinginjava/externalsortinginjava "0.6.6"]
                 [com.github.clojure-finance/clojask-io "1.0.8"]]
  ;; lein deploy clojars, credentials from CLOJARS_USERNAME / CLOJARS_PASSWORD
  :deploy-repositories [["clojars" {:url "https://clojars.org/repo"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases false}]]
  :repl-options {:timeout 180000}
  :source-paths      ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :javac-options ["--release" "17"]
  :jvm-opts ["-XX:+UseG1GC" "-server"]
  :test-paths        ["test/clojask"]
  ;:java-test-paths   ["test/java"]
  ;;:injections [(.. System (setProperty "clojure.core.async.pool-size" "8"))]
  )
