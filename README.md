# Clojask

[![Clojars](https://img.shields.io/clojars/v/com.github.clojure-finance/clojask.svg)](https://clojars.org/com.github.clojure-finance/clojask)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Clojure dataframe library for **larger-than-memory** datasets. Process millions of rows with lazy evaluation and parallel execution—all with familiar dataframe operations.

## Why Clojask?

| Feature | Description |
|---------|-------------|
| **Larger than memory** | Stream data from disk—no need to fit everything in RAM |
| **Lazy & parallel** | Operations are pipelined and executed across multiple threads |
| **Full dataframe API** | Filter, transform, group-by, aggregate, join—everything you expect |
| **File-to-file** | Read CSV in, write CSV out, with built-in IO |
| **Native types** | Works with standard Clojure and Java types |
| **Faster than Dask** | [Benchmarks](https://clojure-finance.github.io/clojask-website/pages-output/about/#benchmarks) show significant speedups on large datasets |

## Quick Start

**Add to project.clj:**
```clojure
[com.github.clojure-finance/clojask "2.0.5"]
```

**Or deps.edn:**
```clojure
com.github.clojure-finance/clojask {:mvn/version "2.0.5"}
```

**Basic example:**
```clojure
(require '[clojask.dataframe :as ck])

;; Load a CSV
(def df (ck/dataframe "employees.csv"))

;; Preview the data
(ck/print-df df)
;; | Employee | EmployeeName | Department | Salary   | UpdateDate |
;; |----------|--------------|------------|----------|------------|
;; | 1        | Alice        | 11         | 300      | 2019/12/21 |
;; | 2        | Bob          | 12         | 400      | 2018/05/23 |
;; ...

;; Set column types
(ck/set-type df "Salary" "double")
(ck/set-type df "UpdateDate" "date:yyyy/MM/dd")

;; Transform data: give Bob a raise
(ck/operate df 
  (fn [name salary] 
    (if (= name "Bob") (+ salary 100) salary))
  ["EmployeeName" "Salary"] 
  "Salary")

;; Compute with 8 threads, output to file
(ck/compute df 8 "results.csv" 
  :select ["Employee" "EmployeeName" "Department" "Salary"])
```

## Operations

![Clojask operations](docs/clojask_functions.png)

*Solid arrows show required sequence; dotted arrows show optional paths.*

**Available operations:**
- **Transform:** `operate`, `set-type`, `set-parser`, `set-formatter`, `rename-col`
- **Filter:** `filter`
- **Reshape:** `group-by`, `aggregate`, `melt`, `sort` (in-memory only)
- **Combine:** `inner-join`, `left-join`, `right-join`, `rolling-join-forward`, `rolling-join-backward`
- **Output:** `compute`, `print-df`, `preview`

## Requirements

- **OS:** macOS or Linux
- **JDK:** 17 or newer (tested on 17, 21, and 25)

**Required JVM flags** (add to `:jvm-opts` in project.clj/deps.edn):
```clojure
:jvm-opts ["--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
           "--enable-native-access=ALL-UNNAMED"]
```

The first flag is required for Agrona (used by Onyx's messaging). The second prevents warnings on JDK 24+ from lz4's native code.

<details>
<summary><strong>Running multiple Clojask processes</strong></summary>

Each `compute` starts embedded ZooKeeper (port 2188) and Aeron (port 40200). To run multiple processes on one machine, configure different ports:

**System properties:**
- `clojask.zookeeper.port`
- `clojask.aeron.port`
- `clojask.aeron.dir`

**Or environment variables:**
- `CLOJASK_ZOOKEEPER_PORT`
- `CLOJASK_AERON_PORT`
- `CLOJASK_AERON_DIR`

</details>

<details>
<summary><strong>Logging</strong></summary>

Onyx writes warnings and errors to `.clojask/clojask.log` in the working directory, rotated at 10 MB.

</details>

## Documentation

- **[API Reference](https://clojure-finance.github.io/clojask-website/posts-output/API/)** — Full documentation for all functions
- **[Examples Repository](https://github.com/clojure-finance/clojask-examples)** — Real-world usage patterns
- **[Aggregation Functions](docs/aggregation%20functions.md)** — Built-in and custom aggregations
- **[Type System](docs/clojask%20types.md)** — Supported data types and parsing

## How It Works

Clojask uses [Onyx](https://github.com/clojure-finance/onyx) (a maintained fork) as a single-machine thread pool. Operations are collected lazily and executed together when you call `compute`, streaming data through your transformation pipeline without loading the full dataset into memory.

## Issues & Feedback

Found a bug or have a question? Check the [existing issues](https://github.com/clojure-finance/clojask/issues) or open a new one.

## License

[MIT](LICENSE)
