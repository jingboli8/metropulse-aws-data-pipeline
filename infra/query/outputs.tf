output "database_name" {
  description = "Glue database containing the curated table contract."
  value       = aws_glue_catalog_database.query.name
}

output "table_name" {
  description = "Empty curated table awaiting Phase 6 partition publication."
  value       = aws_glue_catalog_table.curated.name
}

output "table_root_location" {
  description = "Curated table root; registered partitions will target approved run IDs."
  value       = aws_glue_catalog_table.curated.storage_descriptor[0].location
}

output "workgroup_name" {
  description = "Athena workgroup with enforced results and scan controls."
  value       = aws_athena_workgroup.query.name
}

output "workgroup_result_location" {
  description = "Enforced isolated query-result prefix."
  value       = aws_athena_workgroup.query.configuration[0].result_configuration[0].output_location
}
