(defproject heratepalvelu "0.1.0-SNAPSHOT"
  :description "Herätepalvelu Arvo-kyselylinkkien lähetyksen automatisointiin."
  :repositories [["releases" "https://artifactory.opintopolku.fi/artifactory/oph-sade-release-local"]
                 ["snapshots" "https://artifactory.opintopolku.fi/artifactory/oph-sade-snapshot-local"]]
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [environ "1.1.0"]
                 [clj-http "3.10.0"]
                 [cheshire "5.8.1"]
                 [clj-time "0.15.1"]
                 [com.amazonaws/aws-lambda-java-core "1.2.0"]
                 [com.amazonaws/aws-lambda-java-events "2.2.6"]
                 [software.amazon.awssdk/dynamodb "2.5.54"]
                 [org.apache.logging.log4j/log4j-api "2.11.2"]
                 [org.apache.logging.log4j/log4j-core "2.11.2"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.11.2"]
                 [prismatic/schema "1.1.10"]
                 [hiccup "1.0.5"]
                 [oph/clj-cas "0.5.3-SNAPSHOT"]]
  :aot :all
  :source-paths ["src"]
  :java-source-paths ["src/java"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
