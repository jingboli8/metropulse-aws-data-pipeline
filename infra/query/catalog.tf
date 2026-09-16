resource "aws_glue_catalog_database" "query" {
  name        = local.database_name
  description = "Curated MetroPT-3 analytics catalog for ${var.environment}."
}

resource "aws_glue_catalog_table" "curated" {
  database_name = aws_glue_catalog_database.query.name
  name          = local.table_name
  description   = "One timezone-unknown local timestamped observation of all 15 MetroPT-3 signals."
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL             = "TRUE"
    classification       = "parquet"
    compressionType      = "snappy"
    "projection.enabled" = "false"
    typeOfData           = "file"
  }

  storage_descriptor {
    location                  = "s3://${var.data_lake_bucket_name}/curated/metropt3/"
    input_format              = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format             = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    compressed                = true
    stored_as_sub_directories = false

    columns {
      name    = "record_index"
      type    = "bigint"
      comment = "Required source row identifier."
    }
    columns {
      name    = "event_timestamp_local"
      type    = "timestamp"
      comment = "Required source wall-clock timestamp; timezone is unknown and is not UTC."
    }
    columns {
      name = "tp2"
      type = "double"
    }
    columns {
      name = "tp3"
      type = "double"
    }
    columns {
      name = "h1"
      type = "double"
    }
    columns {
      name = "dv_pressure"
      type = "double"
    }
    columns {
      name = "reservoirs"
      type = "double"
    }
    columns {
      name = "oil_temperature"
      type = "double"
    }
    columns {
      name = "motor_current"
      type = "double"
    }
    columns {
      name = "comp"
      type = "boolean"
    }
    columns {
      name = "dv_electric"
      type = "boolean"
    }
    columns {
      name = "towers"
      type = "boolean"
    }
    columns {
      name = "mpg"
      type = "boolean"
    }
    columns {
      name = "lps"
      type = "boolean"
    }
    columns {
      name = "pressure_switch"
      type = "boolean"
    }
    columns {
      name = "oil_level"
      type = "boolean"
    }
    columns {
      name = "caudal_impulses"
      type = "boolean"
    }
    columns {
      name    = "source_date"
      type    = "date"
      comment = "Required date derived from the timezone-unknown source timestamp."
    }
    columns {
      name    = "event_timestamp_timezone_status"
      type    = "string"
      comment = "Required contract value: unknown."
    }

    ser_de_info {
      name                  = "metropt3-curated-parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }
  }

  partition_keys {
    name    = "year"
    type    = "string"
    comment = "Four-digit year derived from source_date."
  }
  partition_keys {
    name    = "month"
    type    = "string"
    comment = "Two-digit month derived from source_date."
  }
}
