### API DOCUMENTATION

#### Dataframe

##### Features

- Unlimited size

  Theoretically speaking, it supports dataset larger than memory to infinity!

- All native types

  All the datatypes used to store data is native Clojure (or Java) types!

- From file to file

  Integrate IO inside the dataframe. No need to write your own read-in and output functions!

- Distributed (coming soon)

  Most operations could be distributed to different computers in a clusters. See the principle in [Onyx](http://www.onyxplatform.org/)

- Lazy operations

  Some operations will not be executed immediately. Dataframe will intelligently pipeline the operations altogether in computation.

##### Basic Information

- Most operations to the dataframe is performed lazily and all at once with `compute` except `sort`. 
- The dataframe process the data in rows, ie one row in one vector.
- The input dataframe can be larger than memory in size.

##### API

- filter

  Filters the data frame by rows

  | Argument    | Type                           | Function                                                    | Remarks                                                      |
  | ----------- | ------------------------------ | ----------------------------------------------------------- | ------------------------------------------------------------ |
  | `dataframe` | Clojask.DataFrame              | The operated object                                         |                                                              |
  | `columns`   | String / collection of strings | The columns the predicate function to apply to              |                                                              |
  | `predicate` | Function                       | The predicate function to determine if a row should be kept | This function should have the same number of arguments with the above columns and in the same order. Only rows that return `true ` will be kept. |

  **Example**

  ```clojure
  (filter x "Salary" (fn [salary] (<= salary 800)))
  ;; this statement deletes all the rows that have a salary larger than 800
  ```

  

- set-type

  Set the type of a column. So when using the value of that column, it would be in that type.

  | Argument    | Type              | Function            | Remarks                                                      |
  | ----------- | ----------------- | ------------------- | ------------------------------------------------------------ |
  | `dataframe` | Clojask.DataFrame | The operated object |                                                              |
  | `type`      | String            | Type of the column  | The native support types are: int, double, string, date. Note that by default all the column types are string. If you need special parsing function, see `add-parser`. |
  | `column`    | String            | Target columns      | Should be existing columns within the dataframe              |

  **Example**

  ```clojure
  (set-type x "double" "Salary")
  ;; makes the column Salary doubles
  ```

  

- add-parser

  A more flexible way to set type.

  | Argument    | Type              | Function                                                     | Remarks                                                      |
  | ----------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | `dataframe` | Clojask.DataFrame | The operated object                                          |                                                              |
  | `parser `   | function          | The parser function that will parse a string to other types (or even string) | The function should take only one argument which is a string, and the parsed type should be serializable. |
  | `column`    | String            | Target columns                                               | Should be existing columns within the dataframe              |

  **Example**

  ```clojure
  (add-parser x Double/parseDouble "Salary")
  ;; parse all the values in Salary with this function
  ```

#### Remark

**If you have set type of some columns, do not forget to convert it back to int/double/string at the end using the last operation!**

- operate

  In place modification on a single column

  | Argument      | Type              | Function                      | Remarks                                                      |
  | ------------- | ----------------- | ----------------------------- | ------------------------------------------------------------ |
  | `dataframe`   | Clojask.DataFrame | The operated object           |                                                              |
  | `operation`   | function          | Function to be applied lazily | The function should take only one argument which is the value of the below column |
  | `column name` | Keyword           | Target columns                | Should be existing columns within the dataframe              |

  **Example**

  ```clojure
  (set-type x "double" "Salary")
  (operate x - "Salary")
  ;; takes the negative of column Salary
  ```

  

- operate

  Calculate the result and store in a new column

  | Argument         | Type                            | Function                      | Remarks                                                      |
  | ---------------- | ------------------------------- | ----------------------------- | ------------------------------------------------------------ |
  | `dataframe`      | Clojask.DataFrame               | The operated object           |                                                              |
  | `operation`      | function                        | Function to be applied lazily | Argument number should be complied with the column names below, ie *if operation functions takes two arguments, the length of column names should also be 2, and in the same order to be passed to the function* |
  | `column name(s)` | Keyword or collection of String | Target columns                | Should be existing columns within the dataframe              |
  | `new column`     | String                          | Resultant column              | Should be new column other than the dataframe                |

  **Example**

  ```clojure
  (operate x str ["Employee" "EmployeeName"] "new")
  ;; concats the two columns into the "new" column
  ```

  

- sort

  **Immediately** sort the dataframe

  | Argument           | Type                    | Function                 | Remarks                                                      |
  | ------------------ | ----------------------- | ------------------------ | ------------------------------------------------------------ |
  | `dataframe`        | Clojask.DataFrame       | The operated object      |                                                              |
  | `trending list`    | Collection (seq vector) | Indicates the sort order | Example: ["Salary" "+" "Employee" "-"] means that sort the Salary in ascending order, if equal sort the Employee in descending order |
  | `output-directory` | String                  | The output path          |                                                              |

  **Example**

  ```clojure
  (sort y ["+" "Salary"] "resources/sort.csv")
  ;; sort by Salary ascendingly
  ```

  

- compute

  Compute the result. The pre-defined lazy operations will be executed in pipeline, ie the result of the previous operation becomes the argument of the next operation.

  | Argument         | Type              | Function                                                     | Remarks                                                      |
  | ---------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | `dataframe`      | Clojask.DataFrame | The operated object                                          |                                                              |
  | `num of workers` | int (max 8)       | The number of worker instances (except the input and output nodes) | If this argument >= 2, will use [onyx](http://www.onyxplatform.org/) as the distributed platform |
  | `output path`    | String            | The path of the output csv file                              | Could exist or not.                                          |
  | [`exception`]    | boolean           | Whether an exception during calculation will cause termination | Is useful for debugging or detecting empty fields            |
  
  **Example**
  
  ```clojure
  (compute x 8 "../resources/test.csv" :exception true)
  ;; computes all the pre-registered operations
  ```
  
  

- inner-join / left-join / right-join

  Inner / left / right join two dataframes by some columns

  *Remarks:*

  *Join functions are immediate actions, which will be executed at once.*

  *Will automatically pipeline the registered operations and filters.*

  | Argument            | Type               | Function                                                     | Remarks                                           |
  | ------------------- | ------------------ | ------------------------------------------------------------ | ------------------------------------------------- |
  | `dataframe a`       | Clojask.DataFrame  | The operated object                                          |                                                   |
  | `dataframe b`       | Clojask.DataFrame  | The operated object                                          |                                                   |
  | `a columns`         | Clojure.collection | The keys of a to be aligned                                  | Should be existing headers in dataframe a         |
  | `b columns`         | Clojure.collection | The keys of b to be aligned                                  | Should be existing headers in dataframe b         |
  | `number of workers` | int (max 8)        | Number of worker nodes doing the joining                     |                                                   |
  | `distination file`  | string             | The file path to the distination                             | Will be emptied first                             |
  | [`exception`]       | boolean            | Whether an exception during calculation will cause termination | Is useful for debugging or detecting empty fields |

  **Example**

  ```clojure
  (def x (dataframe "path/to/a"))
  (def y (dataframe "path/to/b"))
  
  (inner-join x y ["col a 1" "col a 2"] ["col b 1" "col b 2"] 8 "path/to/distination" :exception true)
  ;; inner join x and y
  
  (left-join x y ["col a 1" "col a 2"] ["col b 1" "col b 2"] 8 "path/to/distination" :exception true)
  ;; left join x and y
  
  (right-join x y ["col a 1" "col a 2"] ["col b 1" "col b 2"] 8 "path/to/distination" :exception true)
  ;; right join x and y
  ```

  