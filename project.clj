(defproject heratepalvelu "0.1.0-SNAPSHOT"
  :description "Herätepalvelu Arvo-kyselylinkkien lähetyksen automatisointiin."
  :repositories [["releases" "https://artifactory.opintopolku.fi/artifactory/oph-sade-release-local"]
                 ["snapshots" "https://artifactory.opintopolku.fi/artifactory/oph-sade-snapshot-local"]]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.0.0"]
                 [org.clojure/core.memoize "1.0.257"]
                 [environ "1.1.0"]
                 [clj-http "3.10.0"]
                 [cheshire "5.8.1"]
                 [clj-time "0.15.1"]
                 [com.amazonaws/aws-xray-recorder-sdk-core "2.4.0"]
                 [com.amazonaws/aws-xray-recorder-sdk-aws-sdk-v2 "2.4.0"]
                 [com.amazonaws/aws-lambda-java-core "1.2.0"]
                 [com.amazonaws/aws-lambda-java-events "2.2.7"]
                 [dev.weavejester/medley "1.8.1"]
                 [software.amazon.awssdk/dynamodb "2.10.56"]
                 [software.amazon.awssdk/ssm "2.10.56"]
                 [software.amazon.awssdk/sqs "2.10.60"]
                 [org.apache.logging.log4j/log4j-api "2.17.0"]
                 [org.apache.logging.log4j/log4j-core "2.17.0"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.17.0"]
                 [prismatic/schema "1.1.11"]
                 [hiccup "1.0.5"]
                 [oph/clj-cas "0.6.2-SNAPSHOT"]
                 [com.googlecode.libphonenumber/libphonenumber "8.12.45"]
                 [org.clojure/data.json "2.3.1"]]
  :managed-dependencies [[software.amazon.awssdk/aws-core "2.10.56"]
                         [io.netty/netty-buffer "4.1.43.Final"]
                         [fi.vm.sade/scala-cas_2.12 "2.2.2-SNAPSHOT"]
                         [org.slf4j/slf4j-api "1.7.28"]
                         [com.fasterxml.jackson.core/jackson-core "2.10.0"]
                         [org.apache.logging.log4j/log4j-api "2.17.0"]
                         [org.apache.logging.log4j/log4j-core "2.17.0"]
                         [org.apache.logging.log4j/log4j-slf4j-impl "2.17.0"]]
  :plugins [[jonase/eastwood "1.2.3"]
            [lein-bikeshed "0.5.2"]
            [lein-cljfmt "0.8.0"]
            [lein-kibit "0.1.8"]]
  :aliases {"checkall" ["do"
                        ["kibit"]
                        ["bikeshed"]
                        ["eastwood"]
                        ["cljfmt" "check"]]}
  :cljfmt {:indents {cond->       [[:inner 0]]
                     definterface [[:inner 0]]
                     defschema    [[:inner 0]]
                     deftest      [[:inner 0]]
                     testing      [[:inner 0]]
                     with-redefs  [[:inner 0]]
                     #".*"        [[:block 0]]}}
  :eastwood {:add-linters [:no-ns-form-found
                           :unused-locals
                           :unused-namespaces
                           :unused-private-vars]}
  :aot :all
  :source-paths ["src"]
  :java-source-paths ["src/java"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :test {:resource-paths ["test/resources"]}})
