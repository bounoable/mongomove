# mongomove

[![PkgGoDev](https://pkg.go.dev/badge/github.com/bounoable/mongomove)](https://pkg.go.dev/github.com/bounoable/mongomove)

This tool imports your databases from one MongoDB server to another. I use this
package when I need to import databases from MongoDB Atlas to my local machine
for development.

## Install

### Binary

```sh
go install github.com/bounoable/mongomove/cmd/mongomove@latest
```

### As a library

```sh
go get github.com/bounoable/mongomove
```

## Import all databases

Import all databases from `mongodb://127.0.0.1:27017` to
`mongodb://127.0.0.1:27018`:

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018
```

## Filter databases by prefix

Import all databases that have the `my_` prefix from `mongodb://127.0.0.1:27017`
to `mongodb://127.0.0.1:27018`:

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -prefix my_
```

## Skip confirmation

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -confirm
```

## Insert documents in batches

Default batch size is 100. Following command inserts documents in batches of 500 documents:

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -b 500
```

## Parallelize import

By default mongomove uses the number of CPUs to parallelize the import, so that
1 CPU is importing 1 database at a time. The following command imports 2
databases concurrently until all databases have been imported:

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -p 2
```

## Verbose output

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -v
```


## License

[MIT](./LICENSE)
