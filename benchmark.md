## Benchmarks

Number of workers = 4

| Operation | Dask (N=1,800,000) (s) | Clojask (N=1,800,000) (s) |
| :---:   | :-: | :-: |
| Element-wise operation | 58.1 | 89.1 |
| Row-wise selection | 48.5 | 89.6 |
| Aggregation | 49.0 | |
| Groupby-aggregate | 49.6 |  |

Note that all benchmarks shown above are inclusive of the time used for importing necssary libraries, loading the dataframe from csv file and ouputing the processed dataframe to csv format.


## System info
```
'platform': 'Darwin',
'platform-release': '20.4.0',
'platform-version': 'Darwin Kernel Version 20.4.0: Thu Apr 22 21:46:47 PDT 2021; root:xnu-7195.101.2~1/RELEASE_X86_64',
'architecture': 'x86_64',
'processor': 'i386',
'ram': '8 GB'
```
