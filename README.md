# mongomove

[![PkgGoDev](https://pkg.go.dev/badge/github.com/bounoable/mongomove)](https://pkg.go.dev/github.com/bounoable/mongomove)

This tool imports your databases from one MongoDB server to another. I use this
package when I need to import databases from MongoDB Atlas to my local machine
for development.

## Install

```sh
go get github.com/bounoable/mongomove
```

## Import all databases

Import all databases from `mongodb://127.0.0.1:27017` to
`mongodb://127.0.0.1:27018`:

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018
```

## Import databases by prefix

Import all databases that have the `my_` prefix from `mongodb://127.0.0.1:27017`
to `mongodb://127.0.0.1:27018`:

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -prefix my_
```

## Skip confirmation

```sh
mongomove -source mongodb://127.0.0.1:27017 -target mongodb://127.0.0.1:27018 -confirm
```

## License

[MIT](./LICENSE)
