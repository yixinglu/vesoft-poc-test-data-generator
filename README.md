# VEsoft PoC Test Data Generator

VEsoft test data generator for WB PoC

## Usage

```go
$ go run main.go -d <directory-csv-to-be-saved>
```

- `<directory-csv-to-be-saved>`: the directory which you want to put these generated csv files on

This program will generate following csv objects:

```go
const (
	userCount    = 20
	clusterCount = 100
	dbCount      = 1000            // 1k
	datasetCount = 50 * 10 * 1000  // 500k
	jobCount     = 200 * 10 * 1000 // 2M
)
```

And these objects defined in [types.go](generator/types.go)

If you want to use [Nebula Importer](https://github.com/vesoft-inc/nebula-importer) to import these generated csv files to nebula, the configure file [wb.yaml](wb.yaml)
would be what you need.
