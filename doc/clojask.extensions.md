### ns: clojask.extensions

Like many popular Python libraries, such as numpy and pandas, third-party users can extend the function of  Clojask by introducing more codes above the basic source code. This attempt is interesting and encouraged. Here is an example of creating such extension functions.

#### Principle Foundation

When defining a clojask.DataFrame using `dataframe` function, one can input a function instead of the path of the source file. This function should produce a sequence. If this sequence is lazy, the theoretical length of the sequence can be infinite. Otherwise, it must have a finite length that is smaller than the memory size.

```
(def x (dataframe #(["col1,col2" "1,2" "3,4"])))
```

Based on this API, we can define the `cbind` and `rbind` function for two csv files.

#### `cbind-csv`

Joins some csv files into a new dataframe by columns.

| Argument   | Type   | Function                        | Remarks                                                   |
| ---------- | ------ | ------------------------------- | --------------------------------------------------------- |
| path-a     | String | The path of the first csv file  | Can be absolute or relative path                          |
| path-b     | String | The path of the second csv file | Can be absolute or relative path                          |
| [path-c's] | String | Target columns                  | Can be absolute or relative path; the number is not fixed |

**Example**

```clojure
;; file a
date,item,price
2010-01-20,1,18.3
2010-01-20,2,38.3
2010-01-23,1,18.9
2010-01-23,2,48.9
2010-01-26,1,19.1
2010-01-26,2,59.1
;; file b
date,cust,Item,sold
2010-01-19,101,2,11
2010-01-22,102,1,7
2010-01-24,102,2,9
2010-01-25,101,2,9
2010-01-26,101,1,10
(def x (cbind "path/to/a" "path/to/b"))
(print-df x)
```

#### `rbind-csv`

Joins some csv files into a new dataframe by rows.

| Argument   | Type   | Function                        | Remarks                                                   |
| ---------- | ------ | ------------------------------- | --------------------------------------------------------- |
| path-a     | String | The path of the first csv file  | Can be absolute or relative path                          |
| path-b     | String | The path of the second csv file | Can be absolute or relative path                          |
| [path-c's] | String | Target columns                  | Can be absolute or relative path; the number is not fixed |

**Example**

```clojure
;; file a
date,item,price
2010-01-20,1,18.3
2010-01-20,2,38.3
2010-01-23,1,18.9
2010-01-23,2,48.9
2010-01-26,1,19.1
2010-01-26,2,59.1
;; file b
date,cust,Item,sold
2010-01-19,101,2,11
2010-01-22,102,1,7
2010-01-24,102,2,9
2010-01-25,101,2,9
2010-01-26,101,1,10
(def x (cbind "path/to/a" "path/to/b"))
(print-df x)
|             date |             item |            price |
|------------------+------------------+------------------|
| java.lang.String | java.lang.String | java.lang.String |
|       2010-01-20 |                1 |             18.3 |
|       2010-01-20 |                2 |             38.3 |
|       2010-01-23 |                1 |             18.9 |
|       2010-01-23 |                2 |             48.9 |
|       2010-01-26 |                1 |             19.1 |
|       2010-01-26 |                2 |             59.1 |
|       2010-01-19 |              101 |                2 |
|       2010-01-22 |              102 |                1 |
|       2010-01-24 |              102 |                2 |
|       2010-01-25 |              101 |                2 |
```

#### **It is also true to create more binding functions for other file types.**

