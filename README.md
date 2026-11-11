# Databasify Sentinel Family

The Databasify engine for servers watched by Sentinel, shared by [engine-valkey](https://github.com/databasifylabs/engine-valkey) and [engine-redis](https://github.com/databasifylabs/engine-redis). One package runs both; a flavor names the server and its tools.

## Installation

```shell
go get github.com/databasifylabs/engine-sentinel-family@v0.2.0
```

```go
import (
	sdk "github.com/databasifylabs/engine-sdk"
	sentinel "github.com/databasifylabs/engine-sentinel-family"
)

func main() {
	os.Exit(sdk.Run(sentinel.New(sentinel.Valkey)))
}
```

Use `sentinel.Redis` for Redis.

## Versions

The family follows the [engine SDK](https://github.com/databasifylabs/engine-sdk) version it is built on: `v0.2.x` requires `engine-sdk v0.2.x`.

## Testing

```shell
go vet ./...
go test -race -count=1 ./...
```
