# flight_delay_hadoop

## How to run
- data is in the input (small sample is already included)
- run.sh is the complied file that can be run directly 
- source code can be open in intellij 

## Result
- result in ouput/ (complete result for the whole data set)


##Ideas for optimization: 
- currently this is standalone mode, for top k delay problem, we should use partitioner to put the same route to the same slave node  to avoid the data skewer 
- different algorithms like approxi top k can be used based on need
