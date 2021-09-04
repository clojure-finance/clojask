## Benchmarks

Number of workers = 4

| Operation | Dask (N=1,800,000) (s) | Dask (N=3,600,000) (s) | Clojask (N=1,800,000) (s) | Clojask (N=3,600,000) (s) |
| :---:   | :-: | :-: | :-: | :-: |
| Element-wise operation | 119.3 | 261.3 | 71.3 | 133.3 |
| Row-wise selection | 115.0 | 232.0 | 72.8 | 145.6 |
| Aggregation | 49.0 | 226.7 | | |
| Groupby-aggregate | 116.7 | 229.3 | 75.9 | 681.3 |
| Left join | 114.7 | 248.7 | | |
| Inner join | 116.7 | 242.0| 1138.8 | |
| Rolling join | - | - | | |

Note that all benchmarks shown above are inclusive of the time used for importing necssary libraries, loading the dataframe from csv file and ouputing the processed dataframe to one single csv.


## System info
```
'platform': 'Darwin',
'platform-release': '20.4.0',
'platform-version': 'Darwin Kernel Version 20.4.0: Thu Apr 22 21:46:47 PDT 2021; root:xnu-7195.101.2~1/RELEASE_X86_64',
'architecture': 'x86_64',
'processor': 'i386',
'ram': '8 GB'
```
