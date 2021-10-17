# sortondisk

Sort on disk algos.

## Install

```
go build
```

## Run

Sort input file by name
```
./sortondisk -input ./test/data.in -name -output result.out
```

Sort input file by address
```
./sortondisk -input ./test/data.in -address -output result.out
```

## Test

```
go test ./...
```

## Code verification

```
./hack/verify.sh
```
