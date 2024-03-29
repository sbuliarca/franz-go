# Benchmark for using kgo.FetchMaxPartitionBytes 

Running the benchmark:
1. Run: ```go test -bench . -benchmem -count 15 -cpu=1 > testrun_normal.txt```
2. Uncomment ```//kgo.FetchMaxPartitionBytes(10*1024*1024),``` on line 53 in ./client_benchmark_test.go
3. Run ```go test -bench . -benchmem -count 15 -cpu=1 > testrun_with_FetchMaxPartitionBytes10MB.txt```
4. Run ```go install golang.org/x/perf/cmd/benchstat@latest```
5. Run ``` benchstat testrun_normal.txt testrun_with_FetchMaxPartitionBytes10MB.txt > benchstat_results.txt```

In the results you can see that there is more than 300% in the memory allocation.
