# sorter

A sort on disk utility proof-of-concept

## Install

```
go build
```

## Run

Sort input file by name
```
./sorter -input ./test/data.in -name -output result.out
```

Sort input file by address
```
./sorter -input ./test/data.in -address -output result.out
```

## Code verification

```
./hack/verify.sh
```
