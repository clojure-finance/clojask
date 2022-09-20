# Learn Clojask

### 1. Overview

As you know, Clojask is a parallel data processing framework. When we design it, we pay much attention to its usability. So do not panic, this document will guide you through all the basic aspects of Clojask. When you finish the tutorial, you will have no problem using it in production.

### 2. Installation



### 3. Creating a DataFrame

The core of Clojask is its **data-frame**. All of the Clojask APIs are defined for it. It just resembles most existing data-frames, such as `data.frame` in R and `pandas.DataFrame` in Python.

You can create a dataframe from a variety of sources.

- Two dimensional vector

  ```
  (def )
  ```







### Hybrid Column & Empty Fields

Take this dataset as an example.

| Employee | EmployeeName | Department | Salary |
| -------- | ------------ | ---------- | ------ |
| 1        | Alice        | 11         | 300    |
| 2        | Bob          | 11         | 34,000 |
| 3        | Carla        |            | 900    |
| 4        | Daniel       | 12         | 1,000  |
| 5        | Evelyn       | 13         | 800    |
| ...      | ...          | ...        | ...    |

The first thing to do after creating this dataframe is to set the type of column **Salary** to `int`.

```clojure
(ck/set-type "Salary" "int")
```

However, when you take a look at the data type of the latest **Salary** column using function `print-df`. It contains both `int` and `string`. This is because some numbers (contains ",") are not recognized by the integer parser, such as 34,000 and 1,000. Therefore, when you operate function to this column, you should accept both `int` and `string` as the input. One way to solve this issue is to create your own parser for integer that ignores comma.

```clojure
(ck/set-parser "Salary" customized-parser)
```

The column will empty fields is just a special case of hybrid column. The **Department** column has an empty field. The type of it is `string` & `nil`. You can tell that empty fields simply contain value `nil`. It is worth notice that 

```clojure
(ck/compute ... :exception true)
```

will also assign `nil` to fields that throw exceptions during execution. Therefore, your operation functions have to consider `nil` input in these two scenarios.

















