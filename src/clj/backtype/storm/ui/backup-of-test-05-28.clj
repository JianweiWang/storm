;;该源码较26号增加了基本的topology吞吐量的计算功能，获取最近10分钟内spout收到的acked的数量m，throughput = m / 600；

(ns backtype.storm.ui.test
  (:use [compojure.core])
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util log])
 ; (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID system-id?]]])
 ; (:use [ring.adapter.jetty :only [run-jetty]])
  ;(:use [clojure.string :only [trim]])
  (:require [backtype.storm.ui.core :as ui-core])
  (:use [backtype.storm.command.rebalance])
  (:use [backtype.storm.ui.helpers])
  (:import [backtype.storm.generated ExecutorSpecificStats
            ExecutorStats ExecutorSummary TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId RebalanceOptions
            KillOptions])
  (:import [wjw.storm.util FilePrinter Test StormMonitor]);;added by wjw.  
  )

;(gen-class
;    : methods [[p [String] void]])

;;hashmap,key:topology-name+bolt-name value:max capacity of all executors of the bolt.
(def topology-executor-map (java.util.HashMap.))

;;key:every executor id value:topology name.
(def executor-topology-map (java.util.HashMap.))

;;hashmap current-bolt-parallism, the current parallism of each bolt is stored in the map.---> key: topology-name+bolt-name, value: parallism.
(def current-bolt-parallism (java.util.HashMap.))

(def sm (StormMonitor.))
;(defn fun [] (println (.toString (.getExecutor (StormMonitor.)))))

;判断给定的executor是否需要rebalance，目前的方法非常简单，当capacity > 0.75时 返回true，否则返回false。
(defn need-adapt? [^ExecutorSummary e](
))

;;以下两个方法为测试方法
(defn need-adapt-true [^ExecutorSummary e]
  (true
   )
  )
(defn need-adapt-false [^ExecutorSummary e]
  (false
    )
  )
(def m2 (java.util.HashMap.))

;;修改compute-executor-capacity，使其支持spout类型
(defn compute-executor-capacity-modified [^ExecutorSummary e]
  (if (nil? e) (println "e is nil")
  (let [stats (.get_stats e)
        stats1 (if (.get_specific stats)
                 ;(println "stats.get_specific() is nil!")
                  (if (.is_set_bolt (.get_specific stats))
                     (-> stats
                     (ui-core/aggregate-bolt-stats true)
                     (ui-core/aggregate-bolt-streams)
                     swap-map-order
                     (get "600"))
                 (-> stats
                   (ui-core/aggregate-spout-stats true)
                   (ui-core/aggregate-spout-streams)
                   swap-map-order
                     (get "600")))
                
                 )
        uptime (nil-to-zero (.get_uptime_secs e))
        window (if (< uptime 600) uptime 600)
        executed (-> stats1 :executed nil-to-zero)
        latency (-> stats1 :execute-latencies nil-to-zero)
        ]
   (if (> window 0)
     (div (* executed latency) (* 1000 window))
     ))))

;;get the bolt which needs to be rebalanced, related information is stored in topology-executor-map.
(defn get-bolt-capacity [^java.util.ArrayList topologies  ^java.util.HashMap topology-executor-map] 
  (let [length (.size topologies)
        i 0
        executor-list-need-adapted (list )]
         (loop [cnt 0 acc 1]
           (if (< cnt length)
             (recur (inc cnt)
               (let [topology (.get topologies  cnt )
                     executors (.get_executors topology)
                     executor-size (.size executors)
                     topology-name (.get_name topology)
                     capacity-bolt-map (java.util.HashMap.)]
                 
                 (loop [cnt1 0 acc 1] 
                   (if(< cnt1 executor-size)
                     (recur (inc cnt1 )
                       (let [executor (.get executors  cnt1 )
                             usage (if ((complement nil?) executor) (compute-executor-capacity-modified executor))
                             component-id (.get_component_id executor)
                            ; usage-in-map (.get capacity-bolt-map (.get_component_id executor))
                            usage-in-map (.get topology-executor-map (str topology-name "-" component-id))
                             
                             ]
                     ;;get max capacity of all executors of each bolt.
                     (if (nil? usage-in-map)
                       (.put topology-executor-map (str topology-name "-" component-id) usage)
                       ;(.put capacity-bolt-map component-id usage)
                       (if (> usage usage-in-map) 
                         (.put topology-executor-map (str topology-name "-" component-id) usage)
                        ; (.put capacity-bolt-map component-id usage))
                       ;(cons e executor-list-need-adapted)
;                       (if ((complement nil?) usage-in-map)
;                         (if (< usage-in-map usage)
;                           (.put capacity-bolt-map component-id usage)
;                           )
;                         )
                       )
                     )
                             )
                     )
                                         
                   )
                ;(.put topology-executor-map topology-name capacity-bolt-map ) 
             )
                     )
                                     
           )
          )
  )
  )
  )

;; get all topologies 
(defn get-topologies []
  (let [topologies (.getTopology sm)] topologies)
  )

;;for test.
;(rebalance "wordcount" "-e" "wordCountBolt=4")

;(get-bolt-capacity (get-topologies (StormMonitor.)) topology-executor-map)
;(println topology-executor-map)

;; rebalance bolt
(defn rebalance-bolt [topology-name bolt-name bolt-parallism]
  (if (and (> bolt-parallism 0) (< bolt-parallism 16)) 
    (println (str topology-name "-" bolt-name " is rebalancing to " bolt-parallism))
    ;(rebalance topology-name "-e" (str bolt-name "=" bolt-parallism))
    ;(println "rebalance is done.")
    )
  )

;;test
(defn rebalance-bolt1 []
  
    (println  "bolt is rebalancing...")
   ; (rebalance topology-name "-e" (str bolt-name "=" bolt-parallism))
    ;(println "rebalance is done.")
  )

;; rebalance worker
(defn rebalance-worker [topology-name worker-num]
  (rebalance topology-name  "-n" worker-num ))

;;init the current-bolt-parallism map,i.e.,value is 1.
(defn init-current-bolt-parallism [^java.util.HashMap current-bolt-parallism]
  (let [topologies (.getTopology sm)]
    (loop [cnt 0 acc 1]
      (if (< cnt (.size topologies))
        (recur (inc cnt) 
                           (let [topology (.get topologies cnt)
                                 executors (.get_executors topology)
                                 executor-size (.size executors)]
                             (loop [cnt1 0 acc 1]
                               (if (< cnt1 executor-size)
                                 (recur (inc cnt1) (.put current-bolt-parallism (str (.get_name topology) "-" (.get_component_id (.get executors cnt1))) 1))
                                 ))))))))

;;do rebalance and wait 5 minutes.
(defn wait-and-rebalance [fun topology-name bolt-name parallism]  
  (fun topology-name bolt-name parallism)
;  (println current-bolt-parallism)
;  (println "printing current-bolt-parallism done.")
  (.put current-bolt-parallism (str topology-name "-" bolt-name) parallism)
  (println current-bolt-parallism)
  (println "printing current-bolt-parallism done.")
  ;(Thread/sleep 180000)
  )

;;check the usage of each executor to decides whether it needs to be rebalanced and how to be rebalanced.
(defn do-rebalance []
  (let [map-length (.size topology-executor-map)
        keys (.keySet topology-executor-map)
        keys (java.util.ArrayList. keys)]
    (loop [cnt 0 acc 1]
      (if (< cnt map-length )  
        (let [key (.get keys cnt)
              usage (.get topology-executor-map key)
              parallism (.get current-bolt-parallism key)
              topology-name (get (clojure.string/split key #"\-") 0)
              bolt-name (get (clojure.string/split key #"\-") 1)]
        (recur (inc cnt) (if  (and (> usage 0.0001) ((complement =) bolt-name "__acker")) (wait-and-rebalance rebalance-bolt topology-name bolt-name (+ parallism 1))
                           ;(if (and (< usage 0.01) ((complement =) bolt-name "__acker")) (wait-and-rebalance rebalance-bolt topology-name bolt-name (- parallism 1)))
                           )))))))

;;get relationship of executors to topologis--->executor-topology-map.
(defn mk-executor-topology-map [^java.util.HashMap executor-topology-map] 
  (let [topologies (.getTopology sm)
        length (.size topologies)
        ]  
      (loop [cnt 0 acc 1]
        (if (< cnt length)
          (recur (inc cnt )
        (let [topology (.get topologies cnt)
              executors (.get_executors topology)
              executors-length (.size executors)]
          (loop  [cnt1 0 acc 1]
            (if ( < cnt1 executors-length )
              (recur (inc cnt1)
           (.put executor-topology-map (str (.get_component_id (.get executors cnt1)) "-" (pretty-executor-info (.get-executor-info (.get executors cnt1)))) (.get_name topology))
)))))))))

;;2014-05-26

;;key:topology-name value: num of tuple per second
(def topology-throughput-map (java.util.HashMap.))

;;get the throughput of a topology
(defn get-throughput-of-topology []
  (let [topologies (get-topologies)
        length (.size topologies)
        ;executor-list-need-adapted (list )
        component-throughput-map (java.util.HashMap.)]
         (loop [cnt 0 acc 1]
           (if (< cnt length)
             (recur (inc cnt)
               (let [topology (.get topologies  cnt )
                     executors (.get_executors topology)
                     executor-size (.size executors)
                     topology-name (.get_name topology)
                     ]                
                 (loop [cnt1 0 acc 1] 
                   (if(< cnt1 executor-size)
                     (recur (inc cnt1 )
                       (let [executor (.get executors  cnt1 )
                             component-id (.get_component_id executor)
                             stats (.get_stats executor)
                             specifics (.get_specific stats)]
                         (if (.is_set_spout specifics)
                        ;   (println component-id)
;                             (println specifics)
                           (.put topology-throughput-map topology-name (/ (.get (.get (.get_acked (.get_spout specifics)) "600") "default") 600))
)))))))))))


;(def topology-throughput-map (java.util.HashMap.))
;;get the throughput of a topology
;(defn get-throughput-of-topology []
;  (let [topologies (get-topologies)
;        length (.size topologies)
;        ;executor-list-need-adapted (list )
;        component-throughput-map (java.util.HashMap.)]
;         (loop [cnt 0 acc 1]
;           (if (< cnt length)
;             (recur (inc cnt)
;               (let [topology (.get topologies  cnt )
;                     executors (.get_executors topology)
;                     executor-size (.size executors)
;                     topology-name (.get_name topology)
;                     ]
;                 (.put topology-throughput-map topology-name (Long/MAX_VALUE))
;                 
;                 (loop [cnt1 0 acc 1] 
;                   (if(< cnt1 executor-size)
;                     (recur (inc cnt1 )
;                       (let [executor (.get executors  cnt1 )
;                             component-id (.get_component_id executor)
;                             stats (if ((complement nil?) executor) (.get_stats executor))
;                              specifics (.get_specific stats)
;                              bolt-or-spout-stats (if (.is_set_bolt specifics) (.get_bolt specifics) (.get_spout specifics))
;                              stream-ms_avg-map (if (.is_set_bolt specifics) (.get (.get_execute_ms_avg bolt-stats) "600") ())
;                              key-set (.keySet stream-ms_avg-map)
;                              execute-ms-avg (.get stream-ms_avg-map (.next (.iterator key-set)))
;                              throughput (* 1000 (/ 1 execute-ms-avg))
;                              throughput-in-map (.get component-throughput-map component-id)]
;                     (if (nil? throughput-in-map)
;                       (.put component-throughput-map component-id throughput)
;                        (.put component-throughput-map component-id (+ (.get component-throughput-map component-id) throughput)))
;                 (let [bolt-ids (.keySet component-throughput-map)
;                       bolt-ids (java.util.ArrayList. bolt-ids)
;                       set-length (.size bolt-ids)]
;                   (loop [cnt2 0 acc 1] (if (< cnt2 set-length) (recur (inc cnt2) 
;                                                                       (let [bolt-throughput (.get component-throughput-map (.get bolt-ids cnt2))
;                                                                             topology-throughput (.get topology-throughput-map topology-name)]
;                                                                         (if (< topology-throughput bolt-throughput) (.put topology-throughput-map topology-name bolt-throughput)))))))))))))))))

;;end 2014-05-26

;;2014-05-27



;;end 2014-05-27


;(mk-executor-topology-map executor-topology-map)
;(println  executor-topology-map)

 ;; Important!!!
;(fun)
;(import '(java.util TimerTask Timer))
;
;(let [task (proxy [TimerTask] []
;             (run [] (println "Here we go!")))]
;  (. (new Timer) (schedule task (long 20)))
;(.schedule (new Timer) task (long 20) (long 2000))
;  )

;;main
(defn main-test [] 
  (init-current-bolt-parallism current-bolt-parallism)
  (mk-executor-topology-map executor-topology-map)
  (get-bolt-capacity (get-topologies) topology-executor-map)
  ;(println  executor-topology-map)
  (println topology-executor-map)
  ;(do-rebalance)
  (get-throughput-of-topology)
  (println topology-throughput-map))
(main-test)
;(defn mymain[] (.start (Thread. #(main-test))))
