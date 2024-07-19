variable "project" {
  description = "Project"
  default     = "swift-handler-429800-t7"
}

variable "credentials" {
  description = "Project credentials"
  default     = "./keys/creds.json"
}

variable "region" {
  description = "Project location"
  default     = "europe-west3"
}

variable "location" {
  description = "Project location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "swift-handler-429800-t7-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
