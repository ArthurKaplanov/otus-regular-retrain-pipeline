variable "yc_config" {
  type = object({
    zone      = string
    folder_id = string
    cloud_id  = string
  })
  description = "Yandex Cloud configuration"
}


variable "instance_name" {
  description = "Name of the MLflow server instance"
  type        = string
}

variable "service_account_id" {
  description = "ID of the service account"
  type        = string
}

variable "ubuntu_image_id" {
  description = "ubuntu image ID"
  type        = string
}

variable "yc_network_name" {
  type        = string
  description = "Name of the network"
}

variable "yc_subnet_name" {
  type        = string
  description = "Name of the custom subnet"
}

variable "instance_user" {
  description = "Name of the user to create on the compute instance"
  type        = string
  default     = "ubuntu"
}

variable "public_key_path" {
  type        = string
  description = "path to ssh public"
}

variable "private_key_path" {
  type        = string
  description = "path to ssh private"
}

variable "s3_endpoint_url" {
  description = "S3 endpoint URL"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 bucket name for MLflow artifacts"
  type        = string
}

variable "s3_dag_bucket_name" {
  description = "S3 bucket name for airflow dag"
  type        = string
}

# variable "s3_access_key" {
#   description = "S3 access key"
#   type        = string
# }

# variable "s3_secret_key" {
#   description = "S3 secret key"
#   type        = string
# }

variable "mlflow_port" {
  description = "Port for MLflow server"
  type        = number
  default     = 5000
}

variable "postgres_password" {
  description = "Password for PostgreSQL database"
  type        = string
  sensitive   = true
}

# PostgreSQL connection variables
variable "postgres_port" {
  description = "PostgreSQL port"
  type        = number
  default     = 6432
}

variable "postgres_db" {
  description = "PostgreSQL database name"
  type        = string
  default     = "mlflow"
}

variable "postgres_user" {
  description = "PostgreSQL username"
  type        = string
  default     = "mlflow"
}

# variable "bucket_name" {
#   description = "bucket for airflow DAG"
# }

# variable "admin_password" {
#   description = "password for airflow user"
#   sensitive   = true
# }

# variable "airfow_name" {
#   description = "name of airflow instance "
# }



variable "yc_subnet_range" {
  type        = string
  description = "CIDR block for the subnet"
}

variable "yc_nat_gateway_name" {
  type        = string
  description = "Name of the NAT gateway"
}

variable "yc_route_table_name" {
  type        = string
  description = "Name of the route table"
}

variable "yc_mlflow_security_group_name" {
  type        = string
  description = "Name of the security group for mlflow"
}

variable "yc_postgres_security_group_name" {
  type        = string
  description = "Name of the security group for postgres"
}

variable "yc_airflow_security_group_name" {
  type        = string
  description = "Name of the security group for airflow"
}

variable "yc_dataproc_security_group_name" {
  type        = string
  description = "Name of the security group for dataproc"
}


# postgres
variable "yc_postgres_cluster_name" {
  type        = string
  description = "Name of the postgres cluster name"
}

variable "deletion_protection" {
  description = "Protection from accidental deletion"
  type        = bool
  default     = false
}

variable "postgres_version" {
  description = "PostgreSQL version"
  type        = string
  default     = "15"
}


variable "resource_preset_id" {
  description = "Resource preset ID for PostgreSQL hosts"
  type        = string
  default     = "s2.micro" # Минимальный размер
}

variable "disk_type_id" {
  description = "Disk type ID for PostgreSQL hosts"
  type        = string
  default     = "network-ssd"
}

variable "disk_size" {
  description = "Disk size in GB"
  type        = number
  default     = 20 # Минимальный размер
}

variable "assign_public_ip" {
  description = "Assign public IP to PostgreSQL host"
  type        = bool
  default     = false
}

variable "airfow_name" {
  type        = string
  description = "name for airflow cluster"
}

variable "admin_password" {
  type        = string
  description = "password for airflow cluster"
}
