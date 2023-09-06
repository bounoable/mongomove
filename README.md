# mongomove

[![PkgGoDev](https://pkg.go.dev/badge/github.com/bounoable/mongomove)](https://pkg.go.dev/github.com/bounoable/mongomove)

`mongomove` is a utility tool that facilitates the migration of databases from
one MongoDB server to another. It's especially handy for transferring data
between remote servers and local development environments.

## Installation

### As a Command-line Tool:
```sh
go install github.com/bounoable/mongomove/cmd/mongomove@latest
```

### As a Library:
```sh
go get github.com/bounoable/mongomove
```

## Usage

### Import All Databases

Move databases from one server to another:
```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018
```

### Filter Databases By Prefix

Migrate databases that start with a specific prefix:
```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -prefix my_
```

### Skip Confirmation

To bypass the confirmation prompt:
```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -confirm
```

### Insert Documents in Batches

Specify the number of documents to process in a single batch (default is 100):

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -b 500
```

### Parallelize the Import Process

Set the number of concurrent database imports. By default, `mongomove` uses the number of CPUs available:
```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -p 2
```

### Disable Index Creation

Prevent the creation of indexes during import:
```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -indexes false
```

### Enable Verbose Output

For a more detailed output:

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -v
```

## License

[MIT](./LICENSE).
