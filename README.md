# minidb
A storage engine similar to bitcask.

## Getting Started

### Installing
To start using minidb, install Go 1.19 or above. Minidb needs go modules. From your project, run the following command

```sh
$ go get github.com/yanghao888/minidb
```
This will retrieve the library.

### Usage

```go
// Open database
dir := filepath.Join(os.TempDir(), "minidb")
opts := minidb.DefaultOptions(dir)
db, err := minidb.Open(opts)
if err != nil {
    fmt.Printf("Error while opening minidb: %v\n", err)
    os.Exit(1)
}
defer db.Close()

// Put key-value
err = db.Put([]byte("name"), []byte("lion"))
if err != nil {
    fmt.Printf("Put value error: %v\n", err)
    return
}

// Get value
val, err := db.Get([]byte("name"))
if err != nil {
    fmt.Printf("Get value error: %v\n", err)
    return
}
fmt.Printf("Value is: %v\n", string(val))

// Delete key
err = db.Delete([]byte("name"))
if err != nil {
    fmt.Printf("Delete value error: %v\n", err)
    return
}
```

## Design

Minidb’s design is based on a paper titled _[Bitcask: A Log-Structured
Hash Table for Fast Key/Value Data][bitcask]_.

[bitcask]: https://riak.com/assets/bitcask-intro.pdf

### Benchmarks
The benchmarking code, and the detailed logs for the benchmarks can be found in the
[benchmark] package.

[benchmark]: https://github.com/yanghao888/minidb/tree/main/benchmark

Execute the following command to run it：
```sh
$ make bench
```

## License
Minidb is licensed under the term of the GPLv2 License.