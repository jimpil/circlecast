(defproject circlecast "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[jedi-time "0.2.1"]
                 [hazel-atom "0.1.4"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.1"]
                                  [org.clojure/data.json "1.0.0"]
                                  ;[com.hazelcast/hazelcast "4.0.1"]
                                  ;[io.atomix/atomix "3.0.8"]
                                  [com.clojure-goes-fast/clj-memory-meter "0.1.2"]]
                   :jvm-opts ["-Djdk.attach.allowAttachSelf"]}}
  :repl-options {:init-ns circlecast.core})
