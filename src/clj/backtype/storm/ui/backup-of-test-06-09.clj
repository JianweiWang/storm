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


(defn fun [name] (println (str "hello " name)))

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
(def topology-executor-level-map (java.util.concurrent.ConcurrentHashMap.))

;;set topology-executor-level-map
(defn set-topology-executor-level-map [executor-id level]
  (let [^java.util.HashMap level-map (.get topology-executor-level-map executor-id)]
    (println "topology-executor-level-map")
    (println topology-executor-level-map)
    (println "level-map")
    (println level-map)
    (println executor-id)
    (println level)
     (.put level-map level 1)
    (.put topology-executor-level-map executor-id level-map))
  )

;;unset topology-executor-level-map
(defn unset-topology-executor-level-map [executor-id level]
  (let [level-map (.get topology-executor-level-map executor-id)]
    (.put level-map level 0)
    (.put topology-executor-level-map executor-id level-map)))

;; queue of bolt info which was rebalanced,used to determine whether there is throughput increasement of responding topology.
(def bolt-queue (java.util.concurrent.LinkedBlockingQueue.))

;; key: topology-name, value:1 or 0. 1 represents that the topology has been rebalanced but not checked if rebalance is good; 0 
;; represents that it has been checked.
(def rebalancing-flag-map (java.util.concurrent.ConcurrentHashMap.))

;;set info when a topology is rebalanced.
(defn set-rebalancing-flag-map [topology-name]
  (.put rebalancing-flag-map topology-name 1))

;;return true if the topology is checked.
(defn is-not-checked? [topology-name]
  (let [flag (.get rebalancing-flag-map topology-name)]
    (if (= flag 1) false true )))

;;unset the info when a rebalanced topology is checked.
(defn unset-rebalancing-flag-map [topology-name] 
  (.put rebalancing-flag-map topology-name 0))

;;key:topology-name value: num of tuple per second
(def topology-throughput-map (java.util.HashMap.))

;;key:topology-name, value:instance of wjw.storm.util.RebalanceInfo
(def rebalance-info-map (java.util.HashMap.))


;; add the rebalance-info to queue when some topology is rebalanced, which is used for checking whether this rebalancing is good.
(defn bolt-queue-add [^RebalanceInfo rebalance-info]
  (.put bolt-queue rebalance-info))

;;以下两个方法为测试方法
(defn need-adapt-true [^ExecutorSummary e]
  (true
   )
  )
(defn need-adapt-false [^ExecutorSummary e]
  (false
    )
  )
;(def m2 (java.util.HashMap.))

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
                            usage-in-map (.get topology-executor-map (str topology-name "-" component-id))]
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
;; rebalance bolt
(defn rebalance-bolt [topology-name bolt-name bolt-parallism]
  (if (and (> bolt-parallism 0) (< bolt-parallism 16)) 
    ;(println (str topology-name "-" bolt-name " is rebalancing to " bolt-parallism))
    (rebalance topology-name "-e" (str bolt-name "=" bolt-parallism))
    ;(println "rebalance is done.")
    )
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
                            (let [uptime-secs (.get_uptime_secs topology)] 
                              (println (str"uptime: " uptime-secs))
                              
                              (if (< uptime-secs 600)
                                (swap! throughput add (/ (.get (.get (.get_acked (.get_spout specifics)) "600") "default") uptime-secs))
                                (swap! throughput add (/ (.get (.get (.get_acked (.get_spout specifics)) "600") "default") 600)))
                              )
                             
                           
                           )))))) )))) @throughput ))

;;update rebalance-info-map and bolt-queue when rebalancing happens,
(defn update-rebalance-info-map [^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name]
 (if (nil? rebalance-info)
   (let [rebalance-info (RebalanceInfo. topology-name  bolt-name last-parallism current-parallism last-throughput)] 
    (.put rebalance-info-map topology-name rebalance-info)
    (bolt-queue-add rebalance-info)
    (println "***************add rebalance-info to bolt-queue"))
    (let [rebalance-info (.setData rebalance-info topology-name bolt-name last-parallism current-parallism last-throughput )]
      (.put rebalance-info-map topology-name rebalance-info)
      (bolt-queue-add rebalance-info)
      (println "***************add rebalance-info to bolt-queue")
      )))

;;do rebalance and wait 5 minutes.
(defn wait-and-rebalance [fun topology-name bolt-name last-parallism parallism]
  (let [topology-throughput (get-throughput-of-topology-by-name topology-name)
        rebalance-info (.get rebalance-info-map topology-name)]
    (update-rebalance-info-map rebalance-info topology-name last-parallism parallism topology-throughput bolt-name)
    (set-rebalancing-flag-map topology-name)
    (.put current-bolt-parallism (str topology-name "-" bolt-name) parallism)
    (fun topology-name bolt-name parallism)    
    (println current-bolt-parallism)
    (println "printing current-bolt-parallism done."))  
    
  ;(Thread/sleep 180000)
  )

;;check the usage of each executor to decides whether it needs to be rebalanced and how to be rebalanced.
(defn do-rebalance []
  (let [map-length (.size topology-executor-map)
        topology-executors (.keySet topology-executor-map)
        topology-executors (java.util.ArrayList. topology-executors)]
    (println "doing rebalance")
    (loop [cnt 0 acc 1]
      (if (< cnt map-length )  
        (let [topology-executor (.get topology-executors cnt)
              usage (.get topology-executor-map topology-executor)
              parallism (.get current-bolt-parallism topology-executor)
              topology-name (get (clojure.string/split topology-executor #"\-") 0)
              bolt-name (get (clojure.string/split topology-executor #"\-") 1)
              rebalance-info (.get rebalance-info-map topology-name)
              not-checked? (is-not-checked? topology-name)]
        (recur (inc cnt) (if not-checked?
                           (if  (and (> usage 0.01) ((complement =) bolt-name "__acker"))
                                   ;[^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name]
                                   (let [current-parallism (+ parallism 1)
                                           topology-throughput (get-throughput-of-topology-by-name topology-name)]
                                     ;(update-rebalance-info-map rebalance-info topology-name parallism current-parallism topology-throughput bolt-name)
                                     (println "go into wait-and-rebalance.....")
                                     (wait-and-rebalance rebalance-bolt topology-name bolt-name parallism (+ parallism 1))
                                     ;(println "test")))
                                   (if (and (< usage 0.001) ((complement =) bolt-name "__acker")) 
                                     (let [current-parallism (- parallism 1)
                                           topology-throughput (get-throughput-of-topology-by-name topology-name)]
                                       ;[^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name]
                                       ;(update-rebalance-info-map rebalance-info topology-name parallism current-parallism topology-throughput bolt-name)
                                       (println "go into wait-and-rebalance.....")
                                       (wait-and-rebalance rebalance-bolt topology-name bolt-name parallism (- parallism 1))
                                       ;(println current-bolt-parallism)
                                       ;(println topology-executor)
                                      ; (println parallism)
                                      ; (println (- parallism 1))
                                       ;(.put (java.util.HashMap.) "abc" 0)
                                       ;(.put current-bolt-parallism topology-executor (- parallism 1))
                                       ))
                                   )))))))))

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

;;check whether a rebalance is good.A rebalance is good if the throughput of the topology increases after rebalancing, otherwise it is bad.
(defn rebalance-good? [^RebalanceInfo rebalance-info]
  (let [topology-name (.getTopologyName rebalance-info)
        bolt-name (.getBoltName rebalance-info)
        last-parallism (.getLastParallism rebalance-info)
        current-parallism (.getCurrentParallism rebalance-info)
        last-throughput (.getLastThroughput rebalance-info)
        current-throughput (get-throughput-of-topology-by-name topology-name)]
    (if (>= current-throughput last-throughput) true false )
    ))

;;get rebalanced topology from bolt-queue.
(defn get-rebalanced-topology []
  ;(println "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
  ;(println bolt-queue)
  (let [^RebalanceInfo rebalance-info (.take bolt-queue)]
   ; (println "ooooooooooooooooooooooooooooooooooo")
    ;(println rebalance-info)
    rebalance-info))

(defn check-rebalance [rebalance-info]
  (if ((complement nil?) rebalance-info)
    (let [topology-name (.getTopologyName rebalance-info)
          bolt-name (.getBoltName rebalance-info)
          executor-id (str topology-name "-" bolt-name)
          current-parallism (.getCurrentParallism rebalance-info)
          last-parallism (.getLastParallism rebalance-info)
          ;is-bolt (.isBolt rebalance-info)
          ;rebalance-fun (if is-bolt 'rebalance-bolt )
          ]
      ;(println "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
      ;(println rebalance-info)
      ;(println "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
    (if (rebalance-good? rebalance-info)
      (let [abc (atom 0)]
        (set-topology-executor-level-map executor-id current-parallism)
        (unset-rebalancing-flag-map topology-name))
           
      (let [abc (atom 0)]
        ;(println "testtesttesttesttesttesttesttesttesttesttesttest")
        (unset-topology-executor-level-map executor-id current-parallism)
        (wait-and-rebalance rebalance-bolt topology-name bolt-name (- last-parallism 1) last-parallism)
      ;;TODO rebalance: decrease the parallism
      )))))

(defn my-timer 
  ( [task ^Long time1 ^Long time2]  
    (.schedule (new java.util.Timer) task time1 time2)
    )
  ([task ^Long time]
    (.schedule (new java.util.Timer) task time)))
;;re
(defn check-rebalance-loop []
  (println topology-executor-level-map)
  (while true
    (do
     ; (if (not (.isEmpty bolt-queue))
        (let [rebalance-info (get-rebalanced-topology)
              rebalance-time (.getRebalancingTime rebalance-info)
              current-time (System/currentTimeMillis)
              delta (- current-time rebalance-time)
              delta (- 300000 delta)
              delay (if (>= delta 0) delta 0)]
          ;(println "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
          ;(println rebalance-info)
          ;(println "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
          (my-timer (proxy [java.util.TimerTask] [] (run [] (check-rebalance rebalance-info))) delay )))))
;)

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
  (println "-------------------parallism of each bolt---------------------------")
  (println current-bolt-parallism)
  (mk-executor-topology-map executor-topology-map sm)
  (get-bolt-capacity sm topology-executor-map)
  (println  executor-topology-map)
  (println "-------------------capacity of each bolt---------------------------")
  (println topology-executor-map)
  (do-rebalance)
  (get-throughput-of-topology sm)
  (println "-------------------throughput of each topology---------------------------")
  (println topology-throughput-map)
  (println (int (get-throughput-of-topology-by-name "wordcount1000tps")))
  (println bolt-queue)
  )

(def sampling-task (proxy [java.util.TimerTask] []
             (run [] (main-test))))

(def test-task (proxy [java.util.TimerTask] []
                 (run [] (println "hello test"))))

(def test-task1 (proxy [java.util.TimerTask] []
                  (run [] (println "hello wjw"))))
(def main-task (proxy [java.util.TimerTask] []
             (run [] (main-test))))
;(defn my-timer [task time1 time2] 
;  (let [task (proxy [java.util.TimerTask] []
;             (run [] (main-test)))]
;(.schedule (new java.util.Timer) task (long 20) (long 10000))
;  ))

(init-current-bolt-parallism current-bolt-parallism (StormMonitor.))
(println topology-executor-level-map)
(my-timer main-task 20 60000)
(Thread/sleep 10000)
(println "=======================thread is starting")
(.start (Thread. #(check-rebalance-loop)))
;(init-topology-executor-level-map)
;(println topology-executor-level-map)
;(my-timer sampling-task 20 3000)
;(my-timer test-task 20 3000)
;(my-timer test-task1 20 9000)
;(println (get-throughput-of-topology-by-name "wordcount-1000tps"))
;(main-test)
;(println (.values rebalance-info-map ))
;(def testmap (java.util.HashMap.))
;(.put testmap "wlw" 23)
;(println testmap)
