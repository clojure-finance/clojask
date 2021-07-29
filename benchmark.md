## Benchmarks

| Operation | Dask (N=1,800,000) (s) | Clojask (N=1,800,000) (ms) |
| :---:   | :-: | :-: |
| Element-wise operation | 34.8 |  |
| Row-wise selection | 32.2 |  |
| Aggregation | 32.4 |  |
| Groupby-aggregate | 33.4 |  |

Note that all benchmarks shown above are inclusive of the time used for importing necssary libraries, loading the dataframe from csv file and ouputing the processed dataframe to csv format.


## System info
```
'platform': 'Windows',
'platform-release': '10',
'platform-version': '10.0.19041',
'architecture': 'AMD64',
'processor': 'Intel64 Family 6 Model 165 Stepping 5, GenuineIntel',
'ram': '32 GB'
```
