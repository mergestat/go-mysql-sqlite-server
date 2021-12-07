## go-mysql-sqlite-server

This is an *experimental* implementation of a SQLite backend for [`go-mysql-server`](https://github.com/dolthub/go-mysql-server) from DoltHub.
The `go-mysql-server` is a "frontend" SQL engine based on a MySQL syntax and wire protocol implementation.
It allows for pluggable "backends" as database and table providers.

This project implements a SQLite backend so that a SQLite database file on disk may be exposed over a MySQL server interface.

```
go run main.go
```
