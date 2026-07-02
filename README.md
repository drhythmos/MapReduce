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



# MapReduce Demo Guide

This guide explains how to build and run the MapReduce project on WSL (Ubuntu).

---

## Prerequisites

- WSL (Ubuntu)
- Go installed in WSL
- Repository cloned locally

---

# Terminal 1 – Coordinator

### 1. Open WSL

```bash
wsl
```

### 2. Navigate to the project

```bash
cd /mnt/c/Users/rakes/MapReduce/main
```

### 3. Verify you're in the correct directory

```bash
pwd
ls
```

Expected output:

```text
/mnt/c/Users/rakes/MapReduce/main

input
mrcoordinator.go
mrworker.go
wc.go
```

### 4. Clean previous outputs

```bash
rm -f mr-*
rm -f wc.so
rm -f /var/tmp/5840-mr-*
```

### 5. Build the Word Count plugin

```bash
go build -buildmode=plugin -o wc.so wc.go
```

### 6. Start the Coordinator

```bash
go run mrcoordinator.go input/*.txt
```

Leave this terminal running.

---

# Terminal 2 – Worker

### 1. Open another WSL terminal

```bash
wsl
```

### 2. Navigate to the project

```bash
cd /mnt/c/Users/rakes/MapReduce/main
```

### 3. Start the Worker

```bash
go run mrworker.go wc.so
```

The worker will execute all Map tasks followed by all Reduce tasks.

Example:

```text
Start map 0
Done map 0
...
Start reduce 9
Done reduce 9
```

---

# View Final Output

After the worker completes, run:

```bash
cat mr-out-*
```

Example output:

```text
party 27
house 42
river 31
apple 15
```

These files contain the final word counts produced by the MapReduce framework.

---

# Project Workflow

```text
Input Files
      │
      ▼
Coordinator
      │
      ▼
Assign Map Tasks (RPC)
      │
      ▼
Workers
      │
      ▼
Intermediate Files (mr-X-Y)
      │
      ▼
Assign Reduce Tasks (RPC)
      │
      ▼
Workers
      │
      ▼
Final Output (mr-out-*)
```

---

# Interview Talking Points

- The **Coordinator** creates tasks, assigns work to workers using RPC, tracks task status, and reassigns timed-out tasks.
- **Workers** continuously request tasks, execute the user-defined `Map` and `Reduce` functions, write intermediate files, and notify the coordinator when finished.
- Intermediate files (`mr-X-Y`) partition map outputs for reducers.
- Final results are written to `mr-out-*`.

---

# Troubleshooting

## Error: `go.mod file not found`

You are not inside the project directory.

Run:

```bash
cd /mnt/c/Users/rakes/MapReduce/main
pwd
```

Expected output:

```text
/mnt/c/Users/rakes/MapReduce/main
```

---

## Error: `dial unix ... connection refused`

The coordinator is not running.

Start the coordinator first:

```bash
go run mrcoordinator.go input/*.txt
```

Then start the worker.

---

## Error: `cannot load plugin wc.so`

Rebuild the plugin:

```bash
go build -buildmode=plugin -o wc.so wc.go
```

---

# Notes

- Always start the **Coordinator** before starting the **Worker**.
- You can start multiple workers in separate terminals to demonstrate parallel execution.
- Clean previous outputs before each run for a fresh demo.
  
