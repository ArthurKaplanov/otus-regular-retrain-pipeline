//
//#Service account
//
resource "yandex_iam_service_account" "mlflow-sa" {
  name        = "mlflow-manager"
  description = "Service account for mlflow"
}

resource "yandex_resourcemanager_folder_iam_member" "mlflow_sa_roles" {
  for_each = toset([
    "storage.viewer",
    "storage.uploader",
  ])

  folder_id = var.yc_config.folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.mlflow-sa.id}"
}

resource "yandex_iam_service_account_static_access_key" "mlflow_static_key" {
  service_account_id = yandex_iam_service_account.mlflow-sa.id
  description        = "Static access key for service account"
}

# ----------------------------------------------------------
// Добавляем sa для airflow and dataproc
resource "yandex_iam_service_account" "airflow-sa" {
  name        = "airflow-manager"
  description = "service account to manage aiflow cluster"
}

resource "yandex_iam_service_account" "dataproc-sa" {
  name        = "dataproc-manager"
  description = "service account to manage dataproc cluster"
}

resource "yandex_resourcemanager_folder_iam_member" "airflow_sa_roles" {
  for_each = toset([
    "managed-airflow.integrationProvider",
    "dataproc.editor",
    "vpc.user",
    "iam.serviceAccounts.user"
  ])

  folder_id = var.yc_config.folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.airflow-sa.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "dataproc_sa_roles" {
  for_each = toset([
    "dataproc.agent",
    "dataproc.provisioner",
    "storage.editor"
  ])

  folder_id = var.yc_config.folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.dataproc-sa.id}"
}

resource "yandex_storage_bucket_iam_binding" "dataproc_storage" {
  bucket = var.s3_dag_bucket_name
  role   = "storage.editor"

  members = [
    "serviceAccount:${yandex_iam_service_account.dataproc-sa.id}"
  ]
}

//
// Сети
//
# Network ресурсы
resource "yandex_vpc_network" "common_network" {
  name = var.yc_network_name
}

resource "yandex_vpc_subnet" "subnet" {
  name           = var.yc_subnet_name
  zone           = var.yc_config.zone
  network_id     = yandex_vpc_network.common_network.id
  v4_cidr_blocks = [var.yc_subnet_range]
  route_table_id = yandex_vpc_route_table.route_table.id
}

resource "yandex_vpc_gateway" "nat_gateway" {
  name = var.yc_nat_gateway_name
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "route_table" {
  name       = var.yc_route_table_name
  network_id = yandex_vpc_network.common_network.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}

resource "yandex_vpc_security_group" "mlflow_sg" {
  name        = var.yc_mlflow_security_group_name
  description = "Security group for mlflow"
  network_id  = yandex_vpc_network.common_network.id

  ingress {
    # можно подключится по ssh
    protocol       = "TCP"
    description    = "SSH"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 22
  }

  ingress {
    # можно подключится на порт mlflow
    protocol       = "TCP"
    description    = "mlflow"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = var.mlflow_port # 5000 port
  }

  # пока общий потом можно сделать для 443 и 6432 порты
  egress {
    protocol       = "ANY"
    description    = "Allow all outbound traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 0
    to_port        = 65535
  }
}

resource "yandex_vpc_security_group" "postgres_sg" {
  name        = var.yc_postgres_security_group_name
  description = "Security group for postgres"
  network_id  = yandex_vpc_network.common_network.id

  ingress {
    protocol    = "TCP"
    description = "MLflow UI"
    # Это не нужно тк мы указали источни трафика - security group
    # v4_cidr_blocks = ["0.0.0.0/0"]
    port              = var.postgres_port # 6432 port
    security_group_id = yandex_vpc_security_group.mlflow_sg.id
  }

  # Убираем потому что лучше через mlflow vm подключится к БД
  # ingress {
  #   protocol       = "TCP"
  #   description    = "Dbeaver"
  #   v4_cidr_blocks = ["0.0.0.0/0"]
  #   port           = var.postgres_port  # 6432 port
  #   # тут хорошо бы ограничить моим ip но я иногда использую vpn поэтому не буду
  # }

  egress {
    protocol       = "ANY"
    description    = "Allow all outbound traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 0
    to_port        = 65535
  }
}

resource "yandex_vpc_security_group" "airflow_sg" {
  name        = var.yc_airflow_security_group_name
  description = "Security group for airflow"
  network_id  = yandex_vpc_network.common_network.id

  egress {
    protocol       = "TCP"
    description    = "Allow all https trafic"
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 443
    to_port        = 443
  }
}

resource "yandex_vpc_security_group" "dataproc_sg" {
  name        = var.yc_dataproc_security_group_name
  description = "Security group for dataproc"
  network_id  = yandex_vpc_network.common_network.id

  ingress {
    protocol          = "ANY"
    description       = "Internal"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }

  ingress { # потом удалить это для машины прокси в этой же сети
    protocol       = "TCP"
    description    = "SSH from My IP"
    port           = 22
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol          = "ANY"
    description       = "Allow all trafic between vm inside security group"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }
  egress {
    protocol       = "TCP"
    description    = "Allow all https traffic to connect s3 "
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 443
    to_port        = 443
  }

  egress {
    protocol       = "TCP"
    description    = "Allow traffic to connect to mlflow "
    v4_cidr_blocks = ["0.0.0.0/0"]
    # может добавить что security gloup - mlflow
    from_port         = 0
    to_port           = var.mlflow_port
    # security_group_id = yandex_vpc_security_group.mlflow_sg.id
  }
}


# --------
#MLflow
resource "yandex_compute_instance" "mlflow_server" {
  name               = var.instance_name
  service_account_id = yandex_iam_service_account.mlflow-sa.id


  scheduling_policy {
    preemptible = true
  }

  platform_id = "standard-v3"
  resources {
    memory        = 4
    cores         = 4
    core_fraction = 100
  }

  boot_disk {
    initialize_params {
      image_id = var.ubuntu_image_id
      size     = 30 # GB
    }
  }

  network_interface {
    subnet_id = yandex_vpc_subnet.subnet.id
    nat       = true
    security_group_ids = [
      yandex_vpc_security_group.mlflow_sg.id
    ]
  }

  metadata = {
    ssh-keys           = "${var.instance_user}:${file(var.public_key_path)}"
    serial-port-enable = "1"
  }

  connection {
    type        = "ssh"
    user        = var.instance_user
    private_key = file(var.private_key_path)
    host        = self.network_interface.0.nat_ip_address
  }

  # Копируем скрипт установки MLflow
  provisioner "file" {
    source      = "${path.module}/scripts/setup_mlflow.sh"
    destination = "/home/${var.instance_user}/setup_mlflow.sh"
  }

  # Копируем конфигурационный файл для MLflow
  provisioner "file" {
    content = templatefile("${path.module}/scripts/mlflow.conf.tpl", {
      s3_endpoint_url   = var.s3_endpoint_url
      s3_bucket_name    = var.s3_bucket_name
      s3_access_key     = yandex_iam_service_account_static_access_key.mlflow_static_key.access_key
      s3_secret_key     = yandex_iam_service_account_static_access_key.mlflow_static_key.secret_key
      mlflow_port       = var.mlflow_port
      postgres_host     = yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn
      postgres_port     = var.postgres_port
      postgres_db       = var.postgres_db
      postgres_user     = var.postgres_user
      postgres_password = var.postgres_password
    })
    destination = "/home/${var.instance_user}/mlflow.conf"
  }

  # Копируем systemd сервис для MLflow
  provisioner "file" {
    source      = "${path.module}/scripts/mlflow.service"
    destination = "/home/${var.instance_user}/mlflow.service"
  }

  # Запускаем скрипт установки
  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/${var.instance_user}/setup_mlflow.sh",
      "/home/${var.instance_user}/setup_mlflow.sh"
    ]
  }
  depends_on = [
    yandex_mdb_postgresql_cluster.postgres_cluster,
    yandex_mdb_postgresql_user.mlflow_user,
    yandex_mdb_postgresql_database.mlflow_db,
  ]

}

# Обновляем .env файл с URL MLflow сервера
resource "null_resource" "update_env_mlflow" {
  triggers = {
    mlflow_server_ip = yandex_compute_instance.mlflow_server.network_interface.0.nat_ip_address
  }

  provisioner "local-exec" {
    command = <<EOT
      # Определяем переменные
      MLFLOW_TRACKING_URI=http://${yandex_compute_instance.mlflow_server.network_interface.0.nat_ip_address}:${var.mlflow_port}

      # Добавляем или обновляем переменную MLFLOW_TRACKING_URI в .env файле
      if grep -q "^MLFLOW_TRACKING_URI=" ../.env; then
        sed -i "s|^MLFLOW_TRACKING_URI=.*|MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI|" ../.env
      else
        echo "MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI" >> ../.env
      fi
    EOT
  }

  depends_on = [
    yandex_compute_instance.mlflow_server
  ]
}

//
//postgres
//
resource "yandex_mdb_postgresql_cluster" "postgres_cluster" {
  name                = var.yc_postgres_cluster_name
  environment         = "PRODUCTION"
  network_id          = yandex_vpc_network.common_network.id
  security_group_ids  = [yandex_vpc_security_group.postgres_sg.id]
  deletion_protection = var.deletion_protection

  config {
    version = var.postgres_version
    resources {
      resource_preset_id = var.resource_preset_id
      disk_type_id       = var.disk_type_id
      disk_size          = var.disk_size
    }

    access {
      data_lens     = false
      web_sql       = true
      serverless    = false
      data_transfer = false
    }

    performance_diagnostics {
      enabled                      = true
      sessions_sampling_interval   = 60
      statements_sampling_interval = 600
    }

    pooler_config {
      pooling_mode        = "TRANSACTION"
      pooler_pool_discard = true
    }
  }

  host {
    zone             = var.yc_config.zone
    subnet_id        = yandex_vpc_subnet.subnet.id
    assign_public_ip = var.assign_public_ip
  }

  maintenance_window {
    type = "WEEKLY"
    day  = "SAT"
    hour = 12
  }
}

resource "yandex_mdb_postgresql_database" "mlflow_db" {
  cluster_id = yandex_mdb_postgresql_cluster.postgres_cluster.id
  name       = var.postgres_db
  owner      = yandex_mdb_postgresql_user.mlflow_user.name
}

resource "yandex_mdb_postgresql_user" "mlflow_user" {
  cluster_id = yandex_mdb_postgresql_cluster.postgres_cluster.id
  name       = var.postgres_user
  password   = var.postgres_password

  settings = {
    default_transaction_isolation = "read committed"
    log_min_duration_statement    = 5000
  }

}

# Обновляем .env файл с данными подключения к PostgreSQL
resource "null_resource" "update_env_postgres" {
  triggers = {
    cluster_id = yandex_mdb_postgresql_cluster.postgres_cluster.id
  }

  provisioner "local-exec" {
    command = <<EOT
      # Определяем переменные
      POSTGRES_HOST=${yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn}
      POSTGRES_PORT=6432
      POSTGRES_DB=${var.postgres_db}
      POSTGRES_USER=${var.postgres_user}
      POSTGRES_PASSWORD=${var.postgres_password}
      POSTGRES_CONNECTION_STRING=postgresql://${var.postgres_user}:${var.postgres_password}@${yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn}:6432/${var.postgres_db}

      # Добавляем или обновляем переменные в .env файле
      if grep -q "^POSTGRES_HOST=" ../../.env; then
        sed -i "s|^POSTGRES_HOST=.*|POSTGRES_HOST=$POSTGRES_HOST|" ../../.env
      else
        echo "POSTGRES_HOST=$POSTGRES_HOST" >> ../../.env
      fi

      if grep -q "^POSTGRES_PORT=" ../../.env; then
        sed -i "s|^POSTGRES_PORT=.*|POSTGRES_PORT=$POSTGRES_PORT|" ../../.env
      else
        echo "POSTGRES_PORT=$POSTGRES_PORT" >> ../../.env
      fi

      if grep -q "^POSTGRES_DB=" ../../.env; then
        sed -i "s|^POSTGRES_DB=.*|POSTGRES_DB=$POSTGRES_DB|" ../../.env
      else
        echo "POSTGRES_DB=$POSTGRES_DB" >> ../../.env
      fi

      if grep -q "^POSTGRES_USER=" ../../.env; then
        sed -i "s|^POSTGRES_USER=.*|POSTGRES_USER=$POSTGRES_USER|" ../../.env
      else
        echo "POSTGRES_USER=$POSTGRES_USER" >> ../../.env
      fi

      if grep -q "^POSTGRES_PASSWORD=" ../../.env; then
        sed -i "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$POSTGRES_PASSWORD|" ../../.env
      else
        echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" >> ../../.env
      fi

      if grep -q "^POSTGRES_CONNECTION_STRING=" ../../.env; then
        sed -i "s|^POSTGRES_CONNECTION_STRING=.*|POSTGRES_CONNECTION_STRING=$POSTGRES_CONNECTION_STRING|" ../../.env
      else
        echo "POSTGRES_CONNECTION_STRING=$POSTGRES_CONNECTION_STRING" >> ../../.env
      fi
    EOT
  }

  depends_on = [
    yandex_mdb_postgresql_cluster.postgres_cluster,
    yandex_mdb_postgresql_database.mlflow_db,
    yandex_mdb_postgresql_user.mlflow_user
  ]
}

//
//Airflow
//
resource "yandex_airflow_cluster" "airflow" {
  airflow_version    = "2.11"
  name               = var.airfow_name
  service_account_id = yandex_iam_service_account.airflow-sa.id
  admin_password     = var.admin_password
  subnet_ids         = [yandex_vpc_subnet.subnet.id]
  security_group_ids = [
    yandex_vpc_security_group.airflow_sg.id
  ]

  code_sync = {
    s3 = {
      bucket = var.s3_dag_bucket_name
    }
  }

  scheduler = {
    count              = 1
    resource_preset_id = "c2-m4"
  }

  webserver = {
    count              = 1
    resource_preset_id = "c2-m4"
  }

  worker = {
    min_count          = 0
    max_count          = 4
    resource_preset_id = "c2-m8"
  }

  airflow_config = {
    "api" = {
      "auth_backends" = "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
    }
    "scheduler" = {
      "dag_dir_list_interval" = "10"
    }
  }

  logging = {
    enabled   = true
    folder_id = var.yc_config.folder_id
    min_level = "INFO"
  }

  depends_on = [
    yandex_resourcemanager_folder_iam_member.airflow_sa_roles
  ]
}

resource "local_sensitive_file" "airflow_config" {
  content = jsonencode({
    YC_ZONE              = var.yc_config.zone
    YC_DATAPROC_SA_ID    = yandex_iam_service_account.dataproc-sa.id
    YC_SUBNET_ID         = yandex_vpc_subnet.subnet.id
    YC_SECURITY_GROUP_ID = yandex_vpc_security_group.dataproc_sg.id
    YC_BUCKET_NAME       = var.s3_dag_bucket_name
    SSH_PUBLIC_KEY       = file(pathexpand(var.public_key_path))
  })
  filename        = "${path.module}/airflow_config.json"
  file_permission = "0600"
}



resource "local_file" "variables_file" {
  content = jsonencode({
    # общие переменные
    YC_ZONE           = var.yc_config.zone
    YC_FOLDER_ID      = var.yc_config.folder_id
    YC_SUBNET_ID      = yandex_vpc_subnet.subnet.id
    YC_SSH_PUBLIC_KEY = file(pathexpand(var.public_key_path))
    # S3
    S3_ENDPOINT_URL = var.s3_endpoint_url
    S3_ACCESS_KEY   = yandex_iam_service_account_static_access_key.mlflow_static_key.access_key
    S3_SECRET_KEY   = yandex_iam_service_account_static_access_key.mlflow_static_key.secret_key
    S3_BUCKET_NAME  = var.s3_bucket_name
    # Data Proc
    DP_SECURITY_GROUP_ID = yandex_vpc_security_group.dataproc_sg.id
    DP_SA_ID             = yandex_iam_service_account.dataproc-sa.id
    # DP_SA_AUTH_KEY_PUBLIC_KEY = module.iam.public_key # не нужен
    # DP_SA_JSON = jsonencode({ # не нужен
    #   id                 = module.iam.auth_key_id
    #   service_account_id = module.iam.service_account_id
    #   created_at         = module.iam.auth_key_created_at
    #   public_key         = module.iam.public_key
    #   private_key        = module.iam.private_key
    # })
    # MLflow
    MLFLOW_TRACKING_URI = "http://${yandex_compute_instance.mlflow_server.network_interface.0.nat_ip_address}:${var.mlflow_port}"
    # PostgreSQL
    POSTGRES_HOST = yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn
    POSTGRES_PORT = var.postgres_port
    POSTGRES_DB   = var.postgres_db
    POSTGRES_USER = var.postgres_user
  })
  filename        = "./variables.json"
  file_permission = "0600"
}
