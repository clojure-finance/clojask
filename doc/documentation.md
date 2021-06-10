### API DOCUMENTATION

#### Dataframe

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