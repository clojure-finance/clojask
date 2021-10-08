## Benchmarks

Number of workers = 4

| Operation | Dask (N=1.8M) | Dask (N=3.6M) | Dask (N=80M)* | Clojask (N=1.8M) | Clojask (N=3.6M) | Clojask (N=80M) |
| :---:   | :-: | :-: | :-: | :-: | :-: | :-: |
| Element-wise operation | 119.3 | 261.3 | N/A | 72.3 | 133.3 | 1836.6 |
| Row-wise selection | 115.0 | 232.0 | N/A | 67.9 | 145.6 | 1757.5 |
| Aggregation | 116.0 | 226.7 | N/A | - | - | - |
| Groupby-aggregate | 116.7 | 229.3 | N/A | 459.5 | 681.3 | |
| Left join | 114.7 | 248.7 | N/A | 1152.0 | 2310.2 | 14007.9 |
| Inner join | 116.7 | 242.0| N/A | 1138.8 | | |
| Rolling join | - | - | - | | | |

**Remarks:**
- N = Number of lines in csv file
- All benchmarks are in the unit of second (s)
- All benchmarks are inclusive of the time used for importing necssary libraries, loading the dataframe from csv file and ouputting the processed dataframe to one single csv file.
- *In the case of Dask (N=80M) the program could not manage to complete the opeartion in 7 hours


## System info
```
'platform': 'Darwin',
'platform-release': '20.4.0',
'platform-version': 'Darwin Kernel Version 20.4.0: Thu Apr 22 21:46:47 PDT 2021; root:xnu-7195.101.2~1/RELEASE_X86_64',
'architecture': 'x86_64',
'processor': 'i386',
'ram': '8 GB'
```
