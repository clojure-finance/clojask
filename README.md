# clojask
Clojure data frame with parallel computing on larger-than-memory datasets

#### Run the main function in `core`:

```
lein run
```

#### Run the tests in `test`:

```
lein test
```


To run a particular test defined in the namespace:
```
lein test :only clojask.core-test/df-api-test 
```

#### Requirements for the input file:
- the first row should contains the column names
