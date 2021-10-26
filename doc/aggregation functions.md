### Aggregation Functions

In Clojask, you can aggregate on the whole dataframe, or on the group-by dataframe. We call the first case "simple aggregation" and the second "group-by aggregation". Some given functions for simple aggregation are defined in namespace `clojask.api.aggregate`, and the given functions for group-by aggregation are defined in namespace `clojask.api.gb-aggregate`. 

Below are full list of given functions for the two types.

#### `clojask.api.aggregate`:

`max`: Find the max value (use `clojure.core/compare` as the comparator)

`min`: Find the min value (use `clojure.core/compare` as the comparator)

#### `clojask.api.gb-aggregate`:

`max`: Find the max value (use `clojure.core/compare` as the comparator)

`min`: Find the min value (use `clojure.core/compare` as the comparator)

Besides these given functions, you are also welcomed to define your own.

#### How to define group-by aggregation functions?

This is the template:

```clojure
(defn aggre-template
  [col]  ;; take only one argument which is the aggregation column in the format of vector
  ;; ... your implementation
  result    ;; return one variable (could be int / double / string / collection of above)
  )
```

Basically, the function should take one argument only, which is the full aggregation column. ***Here we simply assume this column should be smaller than memory!***

You may find many built-in function in Clojure also fulfilling this requirement, for example, `count`, `mean`, and countless function constructed from [`reduce`](https://clojuredocs.org/clojure.core/reduce).

#### How to define simple aggregation functions?

This is the template:

```clojure
(defn gb-aggre-template
  [new-value old-result]
  ;; new-value: the value for the column on the current row
  ;; old-result: the value of the result for the previous gb-aggre-template
  ;; ... your implementation
  new-result   ;; return the new result, and this will be passed as old-result for the next gb-aggre-template
  )
```

**Notes:**

1. The old-result for the first `gb-aggre-template` is `clojask.api.gb-aggregate/start`. So your function must be able to deal with cases when the second argument is `clojask.api.gb-aggregate/start`.
2. Your function should be self-sustainable, meaning that the result of `gb-aggre-template` should be safe as the input for `gb-aggre-template`.