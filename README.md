# Clojask
Clojure data frame with parallel computing on larger-than-memory datasets

### Features

- **Unlimited size**

  Theoretically speaking, it supports dataset larger than memory, even to infinity!

- **Fast**

  Faster than Dask in most operations, and the larger the dataframe is, the bigger the advantage

- **All native types**

  All the datatypes used to store data is native Clojure (or Java) types!

- **From file to file**

  Integrate IO inside the dataframe. No need to write your own read-in and output functions!

- **Parallel**

  Most operations could be executed into multiple threads or even machines. See the principle in [Onyx](http://www.onyxplatform.org/).

- **Lazy operations**

  Most operations will not be executed immediately. Dataframe will intelligently pipeline the operations altogether in computation.

### Installation

Available on [Clojars](https://clojars.org/com.github.clojure-finance/clojask). 

Insert this line into your `project.clj` if using Leiningen.

```
[com.github.clojure-finance/clojask "1.1.0"]
```

Insert this line into your `deps.edn` if using CLI.

```
com.github.clojure-finance/clojask {:mvn/version "1.1.0"}
```

### Documentation

The detailed doc for every API can be found [here](https://clojure-finance.github.io/clojask-website/posts-output/API/).

### Examples

A separate repository for some typical usage of Clojask can be found [here](https://github.com/clojure-finance/clojask-examples).

### Problem Feedback

If your question is not answered in existing [issues](https://github.com/clojure-finance/clojask/issues). Feel free to create a new one.
