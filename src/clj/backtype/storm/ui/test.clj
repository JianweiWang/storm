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
  (:import [wjw.storm.util FilePrinter Test StormMonitor RebalanceInfo MyConcurrentQueue ]);;added by wjw.  
  )

(defn swap [a b] (+ 0 b))

;;hashmap,key:topology-name+bolt-name value:max capacity of all executors of the bolt.
 (def topology-executor-map (java.util.HashMap.))

;;key:every executor id value:topology name.
(def executor-topology-map (java.util.HashMap.))

;;hashmap current-bolt-parallism, the current parallism of each bolt is stored in the map.---> key: topology-name+bolt-name, value: parallism.
(def current-bolt-parallism (java.util.HashMap.))

;;hashmap topology-executor-level-map
(def topology-executor-level-map (java.util.concurrent.ConcurrentHashMap.))

;;hashmap key:topology-name, value: start-time of a topology;
(def topology-start-time (java.util.concurrent.ConcurrentHashMap.))

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

(defn get-bolt-capacity-by-name [tname bname] 
  (let [topologies (.getTopology (StormMonitor.))
        length (.size topologies)
        capacity (atom 0)]
         (loop [cnt 0 acc 1]
           (if (< cnt length)
             (recur (inc cnt)
               (let [topology (.get topologies  cnt )
                     executors (.get_executors topology)
                     executor-size (.size executors)
                     topology-name (.get_name topology)
                     capacity-bolt-map (java.util.HashMap.)]
                 (if (= topology-name tname)
                   (loop [cnt1 0 acc 1] 
                     (if(< cnt1 executor-size)
                       (recur (inc cnt1 )
                         (let [executor (.get executors  cnt1 )
                               usage (if ((complement nil?) executor) (compute-executor-capacity-modified executor))
                               component-id (.get_component_id executor)]
                       ;;get max capacity of all executors of each bolt.
                       (if (= component-id bname)
                         (swap! capacity swap usage))
                       )))))))))@capacity))

;;key: topology-bolt, value: bolt-process-ms-avg last 10 minutes.
(def bolt-process-time-map (java.util.concurrent.ConcurrentHashMap.))
(def spout-complete-time-map (java.util.concurrent.ConcurrentHashMap.))
;;get the bolt which needs to be rebalanced, related information is stored in topology-executor-map.
(defn get-bolt-capacity [] 
  (let [sm (StormMonitor.)
        topologies (.getTopology sm)
        length (.size topologies)
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
                             executor-stat (.get_stats executor)
                             executor-specific (.get_specific executor-stat)
                             is-spout (.is_set_spout executor-specific)
                             usage (if ((complement nil?) executor) (compute-executor-capacity-modified executor))
                             component-id (.get_component_id executor)
                            usage-in-map (.get topology-executor-map (str topology-name "-" component-id))]
                        (if is-spout (let [spout_stats (.get_spout executor-specific)
                                           complete-time-size (.get_complete_ms_avg_size spout_stats)
                                           complete-time (double (/ (apply + (.values (.get (.get_complete_ms_avg spout_stats) "600"))) complete-time-size))
                                           queue (.get spout-complete-time-map topology-name)]
                                       (if (not (nil? queue))
                                         (let [a 0]
                                           (.add queue complete-time)
                                           (.put spout-complete-time-map topology-name queue))
                                         (let [queue (MyConcurrentQueue.)]
                                           (.add queue complete-time)
                                           (.put spout-complete-time-map topology-name queue)))
                                       ;(.put spout-complete-time-map topology-name  complete-time)
                                       )
                                      (let [bolt-stats (.get_bolt executor-specific)
                                            process-time-size (.get_process_ms_avg_size bolt-stats)
                                            process-time (double (/ (apply + (.values (.get (.get_process_ms_avg bolt-stats) "600"))) process-time-size))]
                                        (.put bolt-process-time-map (str topology-name "-" component-id) process-time)))
                     ;;get max capacity of all executors of each bolt.
                     (if (nil? usage-in-map)
                       (.put topology-executor-map (str topology-name "-" component-id) usage)
                       ;(.put capacity-bolt-map component-id usage)
                       (if (> usage usage-in-map) 
                         (.put topology-executor-map (str topology-name "-" component-id) usage)))))))))))))

;; rebalance bolt
(defn rebalance-bolt [topology-name bolt-name bolt-parallism]
  (if (and (> bolt-parallism 0) (< bolt-parallism 16)) 
    ;(println (str topology-name "-" bolt-name " is rebalancing to " bolt-parallism))
    (let [current-time (System/currentTimeMillis)]
      (rebalance "-w" "0" topology-name  "-e" (str bolt-name "=" bolt-parallism))
      (.put topology-start-time topology-name current-time)
      ;(println "rebalance is done.")
    ))
  )

;; rebalance worker
(defn rebalance-worker [topology-name worker-num]
  (let [current-time (System/currentTimeMillis)]
    (rebalance "-w 0 " topology-name  "-n" worker-num )
    (.put topology-start-time topology-name current-time)
    )
  )

(defn init-topology-executor-level-map [level-flag] 
  (loop [cnt 0 acc 1] 
    (if (< cnt 16) 
      (recur (inc cnt)(.put level-flag (+ cnt 1) 1)))))

;;init the current-bolt-parallism map
(defn init-current-bolt-parallism [^java.util.HashMap current-bolt-parallism ^StormMonitor sm]
  (let [topologies (.getTopology sm)]  
    (loop [cnt 0 acc 1]
      (if (< cnt (.size topologies))
        (recur (inc cnt) 
                           (let [topology (.get topologies cnt)
                                 executors (.get_executors topology)
                                 executor-size (.size executors)
                                 topology-name (.get_name topology)
                                 start-time (.get_uptime_secs topology)
                                 start-time (- (System/currentTimeMillis) (* start-time 1000))]
                             (.put topology-start-time topology-name start-time)
                             (loop [cnt1 0 acc 1]
                               (if (< cnt1 executor-size)
                                 (recur (inc cnt1) (let [bolt-name (.get_component_id (.get executors cnt1))
                                                         executor-id (str topology-name "-" bolt-name)
                                                         level-flag (java.util.HashMap.)]
                                                     (init-topology-executor-level-map level-flag)
                                                     (.put topology-executor-level-map executor-id level-flag)
                                                     (if (nil? (.get current-bolt-parallism executor-id)) (.put current-bolt-parallism executor-id 1) 
                                                        (.put current-bolt-parallism executor-id (+ (.get current-bolt-parallism executor-id) 1))))) )
                                 )))))))

;(defn add [throughput delta] (+ throughput delta))

;;get the process time of bolt
;(defn get-bolt-process-time [])
;;get the workload of a topology by topology-name
(defn get-workload-of-topology-by-name [tname]
  ;(println tname)
  (let [ sm (StormMonitor.)
        topologies (.getTopology sm)
        length (.size topologies)
        ;executor-list-need-adapted (list )
        ;component-throughput-map (java.util.HashMap.)
        work-load (atom 0)]
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
                            (let [start-time-secs (int (/ (.get topology-start-time topology-name) 1000))
                                  current-time-ses (int (/ (System/currentTimeMillis) 1000))
                                  uptime-secs (- current-time-ses start-time-secs)
                                  uptime-secs (inc uptime-secs)
                                  ;uptime-secs (.get_uptime_secs topology)
                                  transferred (.get_transferred stats)
                                  transferred_600 (.get transferred "600")
                                  values (.values transferred_600)
                                  work-load-total (apply + values )] 
                              (println (str"uptime: " uptime-secs))
                              
                              (if (< uptime-secs 600)
                                (swap! work-load swap (/ work-load-total uptime-secs))
                                (swap! work-load swap (/ work-load-total 600)))
                              )
                             
                           
                           )))))))))) @work-load ))

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
                            (let [start-time-secs (int (/ (.get topology-start-time topology-name) 1000))
                                  current-time-ses (int (/ (System/currentTimeMillis) 1000))
                                  uptime-secs (- current-time-ses start-time-secs)
                                  uptime-secs (inc uptime-secs)
                                  ;uptime-secs (.get_uptime_secs topology)
                                  spout_stats (.get_spout specifics)
                                  acked (.get_acked spout_stats)
                                  acked_600 (.get acked "600")
                                  values (.values acked_600)
                                  throughput-total (apply + values )] 
                              (println (str"uptime: " uptime-secs))
                              
                              (if (< uptime-secs 600)
                                (swap! throughput swap (/ throughput-total uptime-secs))
                                (swap! throughput swap (/ throughput-total 600)))
                              )
                             
                           
                           )))))) )))) @throughput ))

;;update rebalance-info-map and bolt-queue when rebalancing happens,
(defn update-rebalance-info-map [^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name work-load]
 (if (nil? rebalance-info)
   (let [rebalance-info (RebalanceInfo. topology-name  bolt-name last-parallism current-parallism last-throughput work-load)] 
    (.put rebalance-info-map topology-name rebalance-info)
    (bolt-queue-add rebalance-info)
    (println "***************add rebalance-info to bolt-queue"))
    (let [rebalance-info (.setData rebalance-info topology-name bolt-name last-parallism current-parallism last-throughput work-load)]
      (.put rebalance-info-map topology-name rebalance-info)
      (bolt-queue-add rebalance-info)
      (println "***************add rebalance-info to bolt-queue")
      )))

;;do rebalance .
(defn wait-and-rebalance [fun topology-name bolt-name last-parallism parallism]
  (let [topology-throughput (get-throughput-of-topology-by-name topology-name)
        work-load (get-workload-of-topology-by-name topology-name)
        rebalance-info (.get rebalance-info-map topology-name)]
    (update-rebalance-info-map rebalance-info topology-name last-parallism parallism topology-throughput bolt-name work-load)
    (set-rebalancing-flag-map topology-name)
    (.put current-bolt-parallism (str topology-name "-" bolt-name) parallism)
    (println  (str "capacity: " (get-bolt-capacity-by-name topology-name bolt-name)))
    (fun topology-name bolt-name parallism)    
    ;(println current-bolt-parallism)
   ; (println "printing current-bolt-parallism done.")
   )  
    
  ;(Thread/sleep 180000)
  )

;;return: if the level of given bolt could be set return true, else return false;
(defn is-level-avaliable? [executor-id level]
  (let [level-map (.get topology-executor-level-map executor-id)
        level-flag (.get level-map level)]
    (if (= level-flag 1) true false)))

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
        (recur (inc cnt) (if (and not-checked? (not (= bolt-name "sentenceSpout")))
                           (if  (and (and (> usage 0.2) ((complement =) bolt-name "__acker")) 
                                     (is-level-avaliable? topology-executor (+ parallism 1)))
                                   ;[^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name]
                                   (let [current-parallism (+ parallism 1)
                                           topology-throughput (get-throughput-of-topology-by-name topology-name)]
                                     ;(update-rebalance-info-map rebalance-info topology-name parallism current-parallism topology-throughput bolt-name)
                                     (println "1:go into wait-and-rebalance.....")
                                     (println topology-executor)
                                     (println (str "1:usage: " usage ))
                                     (wait-and-rebalance rebalance-bolt topology-name bolt-name parallism (+ parallism 1))
                                     )
                                     ;(println "test")))
                                   (if (and (and (< usage 0.1) ((complement =) bolt-name "__acker")) 
                                            (is-level-avaliable? topology-executor (- parallism 1)))
                                     (let [current-parallism (- parallism 1)
                                           topology-throughput (get-throughput-of-topology-by-name topology-name)]
                                       ;[^RebalanceInfo rebalance-info topology-name last-parallism current-parallism last-throughput bolt-name]
                                       ;(update-rebalance-info-map rebalance-info topology-name parallism current-parallism topology-throughput bolt-name)
                                       (println "2:go into wait-and-rebalance.....")
                                       (println topology-executor)
                                        (println (str "2:usage: " usage ))
                                       (wait-and-rebalance rebalance-bolt topology-name bolt-name parallism (- parallism 1))
                                       ;(println current-bolt-parallism)
                                       ;(println topology-executor)
                                      ; (println parallism)
                                      ; (println (- parallism 1))
                                       ;(.put (java.util.HashMap.) "abc" 0)
                                       ;(.put current-bolt-parallism topology-executor (- parallism 1))
                                       ))
                                   ))))))))

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
           (.put executor-topology-map (str (.get_component_id (.get executors cnt1))(StormMonitor.) "-" (pretty-executor-info (.get-executor-info (.get executors cnt1)))) (.get_name topology))
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

(defn get-bolt-process-time-by-name [tname]
  (let [ sm (StormMonitor.)
        topologies (.getTopology sm)
        length (.size topologies)]
         (loop [cnt 0 acc 1]
           (if (< cnt length)
             (recur (inc cnt)
               (let [topology (.get topologies  cnt )
                     executors (.get_executors topology)
                     executor-size (.size executors)
                     topology-name (.get_name topology)]
                 ;(println topology-name)
                 (if (= tname topology-name) 
                   (loop [cnt1 0 acc 1] 
                   (if(< cnt1 executor-size)
                     (recur (inc cnt1 )
                       (let [executor (.get executors  cnt1 )
                             component-id (.get_component_id executor)
                             stats (.get_stats executor)
                             specifics (.get_specific stats)]
                         (if (.is_set_bolt specifics)
                            (let [bolt-stats (.get_bolt executor-specific)
                                  process-time-size (.get_process_ms_avg_size bolt-stats)
                                  process-time (double (/ (apply + (.values (.get (.get_process_ms_avg bolt-stats) "600"))) process-time-size))]
                                 (.put bolt-process-time-map (str topology-name "-" component-id) process-time))))
                           )))))) ))))
;;check if the throughput of a topology is increased after rebalanced.
(defn is-throughput-increased? [^RebalanceInfo rebalance-info]
  (let [topology-name (.getTopologyName rebalance-info)
        last-throughput (.getLastThroughput rebalance-info)
        current-throughput (get-throughput-of-topology-by-name topology-name)]
    (if (>= current-throughput last-throughput) true false )
    ))

;;return the change of a topology,return 1 if increasing by 10%, -1 if decreasing 10%, else return 0.
(defn workload-change [^RebalanceInfo rebalance-info]
  (let [topology-name (.getTopologyName rebalance-info)
        last-workload (.getWorkLoad rebalance-info)
        current-workload (get-workload-of-topology-by-name topology-name)
        change (atom 0)
        delta (- current-workload last-workload)
        rate (double (/ delta last-workload))
        ]
    (if (> rate 0.1) 
      (swap! change swap 1)
      (if (< rate -0.1) 
        (swap! change swap -1)))
    @change
    ))

;;check if the capacity of a bolt is nomal.If it is large, the function will return 1.Else if it is small, the function will return -1.If it is nomal, the function will return 0.
(defn is-capacity-nomal? [capacity]
  (let [flag (atom 0)]
    (if (> capacity 0.2) (swap! flag swap 1)
      (if (< capacity 0.1) (swap! flag swap -1)
        (swap! flag swap 0)))
    @flag))

;;check whether a rebalance is good.A rebalance is good if the throughput of the topology increases after rebalancing, otherwise it is bad.
(defn rebalance-good? [^RebalanceInfo rebalance-info]
  (let [topology-name (.getTopologyName rebalance-info)
        bolt-name (.getBoltName rebalance-info)
        executor-id (str topology-name "-" bolt-name)
        throughput-increased? (is-throughput-increased? rebalance-info)
        capacity (get-bolt-capacity-by-name topology-name bolt-name)
        capacity-level (is-capacity-nomal? capacity)
        work-load-change (workload-change rebalance-info)
        return-flag (atom false)
        last-parallism (.getLastParallism rebalance-info)
        current-parallism (.getCurrentParallism rebalance-info)]
    ;(println "current capacity....................................................")
   ; (println capacity)
    ;(println str (work-load-change  "    "  capacity-level))
    (if (or throughput-increased? (= capacity-level 0)) 
      (swap! return-flag not) 
      (if (and (= work-load-change -1) (not throughput-increased?)) 
        (let [a 1]
          (swap! return-flag not)
          (set-topology-executor-level-map executor-id last-parallism))
        (if (= work-load-change 1)
          (set-topology-executor-level-map executor-id current-parallism) 
          ;(unset-topology-executor-level-map executor-id current-parallism)
          )))
    ;(if (@return-flag) (println "rebalance is good!") (println "rebalance is bad!"))
     @return-flag))

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
    (if (rebalance-good? rebalance-info)
      (let [abc (atom 0)]
        (set-topology-executor-level-map executor-id current-parallism)
        (unset-rebalancing-flag-map topology-name))
           
      (let [abc (atom 0)]
        ;(println "testtesttesttesttesttesttesttesttesttesttesttest")
        ;(unset-topology-executor-level-map executor-id current-parallism)
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
 ; (def sm (StormMonitor.))
  ;(println "-------------------parallism of each bolt---------------------------")
  ;(println current-bolt-parallism)
  ;(mk-executor-topology-map executor-topology-map sm)
  (get-bolt-capacity)
  ;(println  executor-topology-map)
  (println bolt-process-time-map)
  ;(println spout-complete-time-map)
  (println (.toString (.get spout-complete-time-map "wordcount")))
  ;(println "-------------------capacity of each bolt---------------------------")
  (println topology-executor-map)
  ;(do-rebalance)
  ;(get-throughput-of-topology sm)
  ;(println "-------------------throughput of each topology---------------------------")
  ;(println topology-throughput-map)
  ;(println )
;  (def file-writer (java.io.FileWriter. "/root/storm/storm_experiment/workload_and_throughput.txt" true))
;  (def throughput (str "throughput: " (int (get-throughput-of-topology-by-name "wordcount"))  "\n"))
;  (def work-load (str "workload: " (int (get-workload-of-topology-by-name "wordcount")) "\n"))
;  (def uptime (str "uptime: " (int (/ (- (System/currentTimeMillis) (.get topology-start-time "wordcount") ) 1000)) "s" "\n"))
;  (print throughput)
;  (print work-load)
;  (.write file-writer uptime)
;  (.write file-writer throughput)
;  (.write file-writer work-load)
;  (.flush file-writer)
;  (.close file-writer)
  ;(println bolt-queue)
;(println (get-bolt-capacity-by-name "wordcount" "sentenceSplitBolt"))
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


;(println topology-executor-level-map)
;(while true (do 
;              (let [time-delay 45000
;                    thread-flag (atom false)
;                    sm (StormMonitor.)]
;                (Thread/sleep time-delay)
;                (init-current-bolt-parallism current-bolt-parallism sm)
;                (main-test sm)
;                (if @thread-flag
;                  (let [time-delay1 10000]
;                    (Thread/sleep time-delay1)
;                    (.start (Thread. #(check-rebalance-loop)))
;                    (swap! thread-flag not)
;                    )))))
(init-current-bolt-parallism current-bolt-parallism (StormMonitor.))
(println topology-start-time)
;(Thread/sleep 30000)
;(let [tname "wordcount"
;      bname "wordCountBolt"
;      parallism 4]
;  (rebalance-bolt tname bname parallism)
;  )
;(Thread/sleep 10000)
(my-timer main-task 20 5000)
;(Thread/sleep 120000)
;(rebalance-bolt "wordcount-dynamic" "wordCountBolt" 4)
;(println "=======================thread is starting")
;(.start (Thread. #(check-rebalance-loop)))
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
