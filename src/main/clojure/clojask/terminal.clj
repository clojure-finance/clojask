(ns clojask.terminal
  )

(defn print-progress
  "Print the progress of perc"
  [perc & {:keys [total init stage] :or {total 25 init false stage "unknown"}}]
  (let [total (or total 25)
        count (int (* perc total))
        rem (- total count)
        per (* 100.0 perc)]
    (if (not= init true)
      (do (print "\33[1A\33[2K")
          (print "\33[1A\33[2K")
          (flush)))
    (if (not= stage nil)
      (println (str "Stage: " stage)))
    (println (format "[%s%s] %.2f%%" (apply str (repeat count "#")) (apply str (repeat rem " ")) per))
    (flush)))