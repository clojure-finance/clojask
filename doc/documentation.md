### API DOCUMENTATION

#### Dataframe

- filter

  Filters the data frame by rows

  | Argument    | Type              | Function                                                    | Remarks                                                      |
  | ----------- | ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------ |
  | `dataframe` | Clojask.DataFrame | The operated object                                         |                                                              |
  | `predicate` | Function          | The predicate function to determine if a row should be kept | This function should take one argument, which is one row in the format of map. Only rows that return `true ` will be kept. |

  **Example**

  ```clojure
  (filter x (fn [row] (<= (Integer/parseInt (:Salary row)) 800)))
  ;; this statement deletes all the rows that have a salary larger than 800
  ```

  

- operate

  In place modification on a single column

  | Argument      | Type              | Function                      | Remarks                                                      |
  | ------------- | ----------------- | ----------------------------- | ------------------------------------------------------------ |
  | `dataframe`   | Clojask.DataFrame | The operated object           |                                                              |
  | `operation`   | function          | Function to be applied lazily | The function should take only one argument which is the value of the below column |
  | `column name` | Keyword           | Target columns                | Should be existing columns within the dataframe              |

- operate

  Calculate the result and store in a new column

  | Argument         | Type                             | Function                      | Remarks                                                      |
  | ---------------- | -------------------------------- | ----------------------------- | ------------------------------------------------------------ |
  | `dataframe`      | Clojask.DataFrame                | The operated object           |                                                              |
  | `operation`      | function                         | Function to be applied lazily | Argument number should be complied with the column names below, ie *if operation functions takes two arguments, the length of column names should also be 2, and in the same order to be passed to the function* |
  | `column name(s)` | Keyword or collection of keyword | Target columns                | Should be existing columns within the dataframe              |
  | `new column`     | Keyword                          | Resultant column              | Should be new column other than the dataframe                |

- compute

  Compute the result. The pre-defined lazy operations will be executed in pipeline, ie the result of the previous operation becomes the argument of the next operation.

  | Argument         | Type              | Function                                                     | Remarks                                                      |
  | ---------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | `dataframe`      | Clojask.DataFrame | The operated object                                          |                                                              |
  | `num of workers` | int (max 8)       | The number of worker instances (except the input and output nodes) | If this argument >= 2, will use [onyx](http://www.onyxplatform.org/) as the distributed platform |
  | `output path`    | String            | The path of the output csv file                              | Could exist or not.                                          |
  | [`exception`]    | boolean           | Whether an exception during calculation will cause termination | Is useful for debugging or detecting empty fields            |

