# Iceberg ProtocolSwappingFileIO

`ProtocolSwappingFileIO` is a custom Iceberg `FileIO` implementation designed for Spark applications. It allows dynamic swapping of URI protocols based on configurable regex patterns.

## Features

- Supports dynamic protocol swapping for file paths using regex.
- Delegates file operations to an underlying Iceberg `FileIO`.
- Maintains original URI format in returned `InputFile` and `OutputFile`.

## Configuration

Define protocol mappings in the Spark configuration using the `protocol.mapping.<regex>` keys. For example:

```properties
.config("spark.sql.catalog.opencatalog.io-impl", "org.apache.iceberg.tools.ProtocolSwitchingFileIO")
.config("spark.sql.catalog.opencatalog.io-impl-delegate", "org.apache.iceberg.azure.adlsv2.ADLSFileIO")
.config("spark.sql.catalog.opencatalog.protocol.mapping.wasb://", "abfs://")
.config("spark.sql.catalog.opencatalog.protocol.mapping.wasbs://", "abfss://")
```
