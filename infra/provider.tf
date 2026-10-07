
terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 1.00"
}

provider "yandex" {
  zone                     = var.yc_config.zone
  folder_id                = var.yc_config.folder_id
  cloud_id                 = var.yc_config.cloud_id
  service_account_key_file = "key.json"

}
