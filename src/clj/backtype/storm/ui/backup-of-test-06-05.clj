;;添加了rebalance-info-map模块，用于存储每次rebalance的信息，信息包含在wjw.storm.util.RebalanceInfo类中
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
  (:import [wjw.storm.util FilePrinter Test StormMonitor RebalanceInfo]);;added by wjw.  
  )

;(gen-class
;    : methods [[p [String] void]])


;(defn fun [] (println (.toString (.getExecutor (StormMonitor.)))))

;判断给定的executor是否需要rebalance，目前的方法非常简单，当capacity > 0.75时 返回true，否则返回false。
(defn need-adapt? [^ExecutorSummary e](
))
;;hashmap,key:topology-name+bolt-name value:max capacity of all executors of the bolt.
 (def topology-executor-map (java.util.HashMap.))

;;key:every executor id value:topology name.
(def executor-topology-map (java.util.HashMap.))

;;hashmap current-bolt-parallism, the current parallism of each bolt is stored in the map.---> key: topology-name+bolt-name, value: parallism.
(def current-bolt-parallism (java.util.HashMap.))

;;hashmap topology-executor-level-map
(def topology-executor-level-map (java.util.HashMap.))

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
(defn get-bolt-capacity [^StormMonitor sm  ^java.util.HashMap topology-executor-map] 
  (let [topologies (.getTopology sm)
        length (.size topologies)
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
;(defn get-topologies []
;  (let [topologies (.getTopology sm)] topologies)
;  )

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

(defn init-topology-executor-level-map [level-flag] 
  (loop [cnt 0 acc 1] 
    (if (< cnt 16) 
      (recur (inc cnt)(.put level-flag (+ cnt 1) 1)))))

;;init the current-bolt-parallism map,i.e.,value is 1.
(defn init-current-bolt-parallism [^java.util.HashMap current-bolt-parallism ^StormMonitor sm]
  (let [topologies (.getTopology sm)]  
    (loop [cnt 0 acc 1]
      (if (< cnt (.size topologies))
        (recur (inc cnt) 
                           (let [topology (.get topologies cnt)
                                 executors (.get_executors topology)
                                 executor-size (.size executors)]
                             (loop [cnt1 0 acc 1]
                               (if (< cnt1 executor-size)
                                 (recur (inc cnt1) (let [executor-id (str (.get_name topology) "-" (.get_component_id (.get executors cnt1)))
                                                         level-flag (java.util.HashMap.)]
                                                     (init-topology-executor-level-map level-flag)
                                                     (.put topology-executor-level-map executor-id level-flag)
                                                     (if (nil? (.get current-bolt-parallism executor-id)) (.put current-bolt-parallism executor-id 1) 
                                                        (.put current-bolt-parallism executor-id (+ (.get current-bolt-parallism executor-id) 1))))) )
                                 )))))))

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

(def rebalance-info-map (java.util.HashMap.))

(defn update-rebalance-info-map [^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name]
 (if (nil? rebalance-info)
    (.put rebalance-info-map topology-name (RebalanceInfo. last-parallism current-parallism last-throughput bolt-name))
    (let [rebalance-info (.setData rebalance-info last-parallism current-parallism last-throughput bolt-name)]
      (.put rebalance-info-map topology-name rebalance-info)
      )))

(defn add [throughput delta] (+ throughput delta))

;;get the throughput of a toplogy by topology-name
(defn get-throughput-of-topology-by-name [tname]
  ;(println tname)
  (let [ sm (StormMonitor.)
        topologies (.getTopology sm)
        length (.size topologies)
        ;executor-list-need-adapted (list )
        ;component-throughput-map (java.util.HashMap.)
        throughput (atom 0)]
         (loop [cnt 0 acc 1]
           (if (< cnt length)
             (recur (inc cnt)
               (let [topology (.get topologies  cnt )
                     executors (.get_executors topology)
                     executor-size (.size executors)
                     topology-name (.get_name topology)
                     ]
                 ;(println topology-name)
                 (if (= tname topology-name) 
                   (loop [cnt1 0 acc 1] 
                   (if(< cnt1 executor-size)
                     (recur (inc cnt1 )
                       (let [executor (.get executors  cnt1 )
                             component-id (.get_component_id executor)
                             stats (.get_stats executor)
                             specifics (.get_specific stats)]
                        ; (println "test")
                         (if (.is_set_spout specifics)
                        ;   (println component-id)
;                             (println specifics)
                             (swap! throughput add (/ (.get (.get (.get_acked (.get_spout specifics)) "600") "default") 600))
                           
                           )))))) )))) @throughput ))

;;check the usage of each executor to decides whether it needs to be rebalanced and how to be rebalanced.
(defn do-rebalance []
  (let [map-length (.size topology-executor-map)
        topology-executors (.keySet topology-executor-map)
        topology-executors (java.util.ArrayList. topology-executors)]
    (loop [cnt 0 acc 1]
      (if (< cnt map-length )  
        (let [topology-executor (.get topology-executors cnt)
              usage (.get topology-executor-map topology-executor)
              parallism (.get current-bolt-parallism topology-executor)
              topology-name (get (clojure.string/split topology-executor #"\-") 0)
              bolt-name (get (clojure.string/split topology-executor #"\-") 1)
              rebalance-info (.get rebalance-info-map topology-name)]
        (recur (inc cnt) (if  (and (> usage 0.01) ((complement =) bolt-name "__acker"))
                           ;[^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name]
                           (let [current-parallism (+ parallism 1)
                                   topology-throughput (get-throughput-of-topology-by-name topology-name)]
                             (update-rebalance-info-map rebalance-info topology-name parallism current-parallism topology-throughput bolt-name)
                             ;(wait-and-rebalance rebalance-bolt topology-name bolt-name (+ parallism 1))
                             (println "test")
                             (.put current-bolt-parallism topology-executor (+ parallism 1)))
                           (if (and (< usage 0.001) ((complement =) bolt-name "__acker")) 
                             (let [current-parallism (- parallism 1)
                                   topology-throughput (get-throughput-of-topology-by-name topology-name)]
                               ;[^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name]
                               (update-rebalance-info-map rebalance-info topology-name parallism current-parallism topology-throughput bolt-name)
                               ;(wait-and-rebalance rebalance-bolt topology-name bolt-name (- parallism 1))
                               ;(println current-bolt-parallism)
                               ;(println topology-executor)
                              ; (println parallism)
                              ; (println (- parallism 1))
                               ;(.put (java.util.HashMap.) "abc" 0)
                               (.put current-bolt-parallism topology-executor (- parallism 1))
                               ))
                           )))))))

;;get relationship of executors to topologis--->executor-topology-map.
(defn mk-executor-topology-map [^java.util.HashMap executor-topology-map ^StormMonitor sm] 
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

;;2014-05-26----2014-05-27

;;key:topology-name value: num of tuple per second
(def topology-throughput-map (java.util.HashMap.))

;;get the throughput of a topology
(defn get-throughput-of-topology [^StormMonitor sm]
  (let [topologies (.getTopology sm)
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
;;end 2014-05-26----2014-05-27

;;2014-05-28



;;end 2014-05-28

;;2014-06-03

;;end 2014-06-03
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
(def init-flag 0)
;;main
(defn main-test [] 
  (def sm (StormMonitor.))
  ;(println "-------------------parallism of each bolt---------------------------")
  ;(println current-bolt-parallism)
  (mk-executor-topology-map executor-topology-map sm)
  (get-bolt-capacity sm topology-executor-map)
  ;(println  executor-topology-map)
  ;(println "-------------------capacity of each bolt---------------------------")
  ;(println topology-executor-map)
  (do-rebalance)
  (get-throughput-of-topology sm)
  ;(println "-------------------throughput of each topology---------------------------")
  ;(println topology-throughput-map)
  )

(def sampling-task (proxy [java.util.TimerTask] []
             (run [] (main-test))))


;(defn my-timer [task time1 time2] 
;  (let [task (proxy [java.util.TimerTask] []
;             (run [] (main-test)))]
;(.schedule (new java.util.Timer) task (long 20) (long 10000))
;  ))

(defn my-timer [task ^Long time1 ^Long time2] 
  
(.schedule (new java.util.Timer) task time1 time2)
  )

(init-current-bolt-parallism current-bolt-parallism (StormMonitor.))
;(init-topology-executor-level-map)
;(println topology-executor-level-map)
;(my-timer sampling-task 20 10000)
;(println (get-throughput-of-topology-by-name "wordcount-1000tps"))
(main-test)
(println (.values rebalance-info-map ))
;(def testmap (java.util.HashMap.))
;(.put testmap "wlw" 23)
;(println testmap)
