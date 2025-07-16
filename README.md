# MapReduce

A simplified distributed MapReduce system built from scratch in Go, featuring a coordinator and multiple workers. Handles task distribution, fault tolerance, and parallel processing, resembling the concepts from the classic [MapReduce Paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf). This is a lab in the MIT distributed systems course.

My motivation behind building this was to understand the inner workings of MapReduce. 

## Usage

Build the plugin
```bash
go build -buildmode=plugin wc.go
```

Run the master node in a terminal window
```bash
go run mrcoordinator.go input/pg*.txt
```

Run the worker node in another terminal window. You can run multiple such workers

```bash
go run mrworker.go wc.so
```

You can similarly run other map reduce applications by writing a map and reduce function. Refer [wc.go](/main/wc.go) to see how to write it.
