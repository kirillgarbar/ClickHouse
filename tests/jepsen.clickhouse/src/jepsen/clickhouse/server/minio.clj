(ns jepsen.clickhouse.server.minio
  (:require [clojure.tools.logging :refer :all]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.clickhouse.constants :refer :all]
            [jepsen.clickhouse.server.utils :refer :all]
            [jepsen.clickhouse.utils :as chu]))

(defn minio-alive?
  [node test]
  (info "Checking Minio alive on" node)
  (try
    (c/exec (str root-folder "/mc") :alias :set "'myminio'" "'http://localhost:9000'" "'minioadmin'" "'minioadmin'")
    (catch Exception _ false)))

(defn setup-minio!
  [test node]
  (chu/prepare-dirs)
  (chu/non-precise-cached-wget! "https://dl.min.io/server/minio/release/linux-amd64/minio")
  (chu/non-precise-cached-wget! "https://dl.min.io/client/mc/release/linux-amd64/mc")
  (chu/chmod-binary (str root-folder "/minio"))
  (chu/chmod-binary (str root-folder "/mc"))
  (c/su
   (cu/start-daemon!
    {:pidfile pid-file-path
     :chdir root-folder
     :logfile stderr-file}
    (str root-folder "/minio")
    "server"
    (str data-dir "/data")))
  (chu/wait-clickhouse-alive! node test minio-alive?)
  (c/exec (str root-folder "/mc") :alias :set "'myminio'" "'http://localhost:9000'" "'minioadmin'" "'minioadmin'")
  (c/exec (str root-folder "/mc") :mb :-p "myminio/cloud-storage"))

(defn teardown-minio!
  [reuse-binary]
  (chu/kill-daemon! "minio")
  (chu/clear-dirs reuse-binary))
