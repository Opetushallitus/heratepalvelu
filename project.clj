(defproject heratepalvelu "0.1.0-SNAPSHOT"
  :description "Herätepalvelu Arvo-kyselylinkkien lähetyksen automatisointiin."
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [environ "1.1.0"]
                 [clj-http "3.9.1"]
                 [cheshire "5.8.1"]
                 [com.amazonaws/aws-lambda-java-core "1.2.0"]
                 [com.amazonaws/aws-lambda-java-events "2.2.6"]
                 [com.taoensso/faraday "1.9.0"]]

  :aot :all
  :source-paths ["src"]
  :java-source-paths ["src/java"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
