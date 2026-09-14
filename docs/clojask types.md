## Clojask Types

### Supported Types

string

int

double

date

datetime

### string

The default type for all columns

Class: ` java.lang.String`

#### Examples

```clojure
(set-type dataframe "col-name" "string")
```

### int

Most efficiently stores an integer

Class: `java.lang.Integer`

#### Examples

```clojure
(set-type dataframe "col-name" "int")
```

### double

Accepts floats and integers

Class: `java.lang.Double`

#### Examples

```clojure
(set-type dataframe "col-name" "double")
```

### date

Transform a date string (no time field)

Class: `java.util.Date` (default format string: `yyyy-MM-dd`)

#### Examples

```clojure
;; if the date looks like this 2020/11/12
(set-type dataframe "col-name" "date:yyyy/MM/dd")
```

### datetime

Transform a date-time string

Class: `java.util.Date` (default format string: `yyyy-MM-dd`, so give a pattern that includes the time fields)

#### Examples

```clojure
;; if the date looks like this 2020/11/12 12:12:36
(set-type dataframe "col-name" "datetime:yyyy/MM/dd HH:mm:ss")
```
