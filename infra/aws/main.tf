# Test-session cluster for the vector search service (BUILDLOG step 46, docs/AWS.md).
# NOT APPLIED. Step 47 is pending owner approval; `terraform apply` creates billable resources.
#
#   terraform init && terraform plan -var key_name=<ec2 key pair> -var my_ip=<a.b.c.d>/32
#   terraform apply ...      # starts billing
#   terraform destroy ...    # stop billing as soon as the session ends
#
# Everything is in one AZ and one cluster placement group: intra-AZ traffic is free and latency is
# what a single datacenter gives. Kafka and Redis are self-hosted on one small instance (see docs/AWS.md
# for why MSK/ElastiCache are not used for a 3-4 hour test).

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

variable "region" { default = "us-east-1" }
variable "az" { default = "us-east-1a" }
variable "key_name" { type = string }
variable "my_ip" { type = string } # SSH + Grafana from the owner's machine only

variable "shard_type" { default = "m7g.2xlarge" }       # 8 vCPU / 32 GiB: ~1.34M vectors per node in heap
variable "coordinator_type" { default = "c7g.4xlarge" } # 16 vCPU: query embedding is CPU bound
variable "loadgen_type" { default = "c7g.2xlarge" }     # k6
variable "infra_type" { default = "m7g.large" }         # Kafka (KRaft, 1 broker) + Redis + Prometheus/Grafana
variable "worker_type" { default = "c7g.4xlarge" }      # embed-workers, only during the 2M load
variable "worker_count" { default = 2 }

provider "aws" { region = var.region }

data "aws_ami" "al2023_arm" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-arm64"]
  }
}

resource "aws_placement_group" "vs" {
  name     = "vector-search"
  strategy = "cluster"
}

resource "aws_vpc" "vs" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "vs" {
  vpc_id                  = aws_vpc.vs.id
  cidr_block              = "10.42.1.0/24"
  availability_zone       = var.az
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "vs" { vpc_id = aws_vpc.vs.id }

resource "aws_route_table" "vs" {
  vpc_id = aws_vpc.vs.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.vs.id
  }
}

resource "aws_route_table_association" "vs" {
  subnet_id      = aws_subnet.vs.id
  route_table_id = aws_route_table.vs.id
}

resource "aws_security_group" "vs" {
  vpc_id = aws_vpc.vs.id
  ingress { # everything inside the cluster
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip]
  }
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = [var.my_ip]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

locals {
  common = {
    ami                    = data.aws_ami.al2023_arm.id
    subnet_id              = aws_subnet.vs.id
    vpc_security_group_ids = [aws_security_group.vs.id]
    key_name               = var.key_name
    placement_group        = aws_placement_group.vs.id
  }
  # Docker + compose; the images come from `./gradlew jib` (step 45) pushed to a registry, or are built on the box.
  bootstrap = <<-EOT
    #!/bin/bash
    dnf install -y docker
    systemctl enable --now docker
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -sL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-aarch64 \
      -o /usr/local/lib/docker/cli-plugins/docker-compose && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
  EOT
}

resource "aws_instance" "infra" {
  ami                    = local.common.ami
  instance_type          = var.infra_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name
  placement_group        = local.common.placement_group
  user_data              = local.bootstrap
  root_block_device {
    volume_type = "gp3"
    volume_size = 30 # Kafka log for 2M chunks is ~1 GB
  }
  tags = { Name = "vs-infra" }
}

resource "aws_instance" "shard" {
  count                  = 3
  ami                    = local.common.ami
  instance_type          = var.shard_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name
  placement_group        = local.common.placement_group
  user_data              = local.bootstrap
  root_block_device {
    volume_type = "gp3"
    volume_size = 40 # ~1.6-1.8 GB of fp16 snapshots per node (step 37) + WAL + one partition rewrite
  }
  tags = { Name = "vs-shard-${count.index + 1}" }
}

resource "aws_instance" "coordinator" {
  ami                    = local.common.ami
  instance_type          = var.coordinator_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name
  placement_group        = local.common.placement_group
  user_data              = local.bootstrap
  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }
  tags = { Name = "vs-coordinator" }
}

resource "aws_instance" "loadgen" {
  ami                    = local.common.ami
  instance_type          = var.loadgen_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name
  placement_group        = local.common.placement_group
  user_data              = "${local.bootstrap}\ndnf install -y https://dl.k6.io/rpm/repo.rpm && dnf install -y k6\n"
  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }
  tags = { Name = "vs-loadgen" }
}

# Embed-workers exist only while the 2M chunks are loaded; destroy them with
#   terraform apply -var worker_count=0
resource "aws_instance" "worker" {
  count                  = var.worker_count
  ami                    = local.common.ami
  instance_type          = var.worker_type
  subnet_id              = local.common.subnet_id
  vpc_security_group_ids = local.common.vpc_security_group_ids
  key_name               = local.common.key_name
  placement_group        = local.common.placement_group
  user_data              = local.bootstrap
  root_block_device {
    volume_type = "gp3"
    volume_size = 20
  }
  tags = { Name = "vs-worker-${count.index + 1}" }
}

output "private_ips" {
  value = {
    infra       = aws_instance.infra.private_ip
    shards      = aws_instance.shard[*].private_ip
    coordinator = aws_instance.coordinator.private_ip
    loadgen     = aws_instance.loadgen.private_ip
    workers     = aws_instance.worker[*].private_ip
  }
}

output "grafana" { value = "http://${aws_instance.infra.public_ip}:3000" }
