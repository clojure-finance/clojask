(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'com.github.clojure-finance/clojask)
(def version "2.1.3")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn javac [_]
  (b/javac {:src-dirs ["src/main/java"]
            :class-dir class-dir
            :basis @basis
            :javac-opts ["--release" "17"]}))

(defn jar [_]
  (javac nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :scm {:url "https://github.com/clojure-finance/clojask"
                      :connection "scm:git:git://github.com/clojure-finance/clojask.git"
                      :developerConnection "scm:git:ssh://git@github.com/clojure-finance/clojask.git"
                      :tag version}
                :basis @basis
                :src-dirs ["src/main/clojure"]
                :pom-data [[:licenses
                            [:license
                             [:name "MIT"]
                             [:url "https://github.com/clojure-finance/clojask/blob/2.x.x/LICENSE"]]]
                           [:description
                            "Data analysis and manipulation library with parallel computing for larger-than-memory datasets"]
                           [:url "https://github.com/clojure-finance/clojask"]]})
  (b/copy-dir {:src-dirs ["src/main/clojure"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

;; Reads CLOJARS_USERNAME / CLOJARS_PASSWORD from the environment.
(defn deploy [_]
  (dd/deploy {:installer :remote
              :artifact jar-file
              :pom-file (b/pom-path {:lib lib
                                     :class-dir class-dir})}))
