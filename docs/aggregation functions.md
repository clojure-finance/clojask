### Aggregation Functions

In Clojask, you can aggregate on the whole dataframe, or on the group-by dataframe. We call the first case "simple aggregation" and the second "group-by aggregation". Some given functions for simple aggregation are defined in namespace `clojask.api.aggregate`, and the given functions for group-by aggregation are defined in namespace `clojask.api.gb-aggregate`. 

Below are full list of given functions for the two types.

#### `clojask.api.aggregate`:

`max` / `min`: Find the max / min value (use `clojure.core/compare` as the comparator)

`sum` / `count`: Sum / count the values

`smallest3` / `largest3`: The three smallest / largest values

`smallestk` / `largestk`: The k smallest / largest values; they take k as a third argument, so wrap them, e.g. `#(agg/largestk %1 %2 5)`

#### `clojask.api.gb-aggregate`:

`max` / `min`: Find the max / min value (use `clojure.core/compare` as the comparator)

`sum` / `count`: Sum / count the values

`mean` / `median`: Arithmetic mean / median, as doubles

`mode`: The most frequent values

`sd`: Sample standard deviation, as a double

`skew`: Pearson's second skewness coefficient; NaN when all values are equal

`smallest3` / `largest3`: The three smallest / largest values

`smallestk` / `largestk`: The k smallest / largest values; they take k as a second argument, so wrap them, e.g. `#(gb-agg/largestk % 5)`

Besides these given functions, you are also welcomed to define your own.

#### How to define group-by aggregation functions?

This is the template:

```clojure
(defn gb-aggre-template
  [col]  ;; take only one argument which is the aggregation column in the format of vector
  ;; ... your implementation
  result    ;; return one variable (could be int / double / string / collection of above)
  )
```

Basically, the function should take one argument only, which is the full aggregation column. ***Here we simply assume this column should be smaller than memory!***

You may find many built-in functions in Clojure also fulfilling this requirement, for example `count`, and the many functions constructed from [`reduce`](https://clojuredocs.org/clojure.core/reduce).

#### How to define simple aggregation functions?

This is the template:

```clojure
(defn aggre-template
  [old-result new-value]
  ;; old-result: the value of the result for the previous aggre-template
  ;; new-value: the value for the column on the current row
  ;; ... your implementation
  new-result   ;; return the new result, and this will be passed as old-result for the next aggre-template
  )
```

**Notes:**

1. The old-result for the first `aggre-template` is `clojask.api.aggregate/start`. So your function must be able to deal with cases when the first argument is `clojask.api.aggregate/start`.
2. Your function should be self-sustainable, meaning that the result of `aggre-template` should be safe as the input for `aggre-template`.
   1. To better understand this template, you may refer to the documentation of [`reduce`](https://clojuredocs.org/clojure.core/reduce), the `aggre-template` should be able to use in `reduce`.
3. If the dataframe has no rows, the result is an empty cell.

