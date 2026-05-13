# ============================================================
# ECS Cluster & Services
# ============================================================

resource "aws_ecs_cluster" "main" {
  name = "${var.project}-${var.environment}-cluster"
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name = aws_ecs_cluster.main.name
  capacity_providers = ["FARGATE"]
}

# --- Shared Task Execution Role ---
# (Usually provided by IAM module, but we need it here)

# --- Security Group for ECS Tasks ---
resource "aws_security_group" "ecs_tasks" {
  name        = "${var.project}-ecs-tasks"
  description = "Allow inbound traffic for ECS tasks"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # Flink UI
  ingress {
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Kafka UI
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- Service Discovery (Cloud Map) ---
resource "aws_service_discovery_private_dns_namespace" "main" {
  name        = "${var.project}.local"
  vpc         = var.vpc_id
  description = "Service discovery for ad-analytics"
}

# ============================================================
# KAFKA SERVICE (Fargate)
# ============================================================
resource "aws_service_discovery_service" "kafka" {
  name = "kafka"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 60
      type = "A"
    }
  }
}

resource "aws_ecs_task_definition" "kafka" {
  family                   = "${var.project}-kafka"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  container_definitions = jsonencode([
    {
      name  = "kafka"
      image = "confluentinc/cp-kafka:7.6.0"
      portMappings = [
        { containerPort = 9092, hostPort = 9092 },
        { containerPort = 9093, hostPort = 9093 }
      ]
      environment = [
        { name = "KAFKA_NODE_ID", value = "1" },
        { name = "KAFKA_PROCESS_ROLES", value = "broker,controller" },
        { name = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", value = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT" },
        { name = "KAFKA_CONTROLLER_QUORUM_VOTERS", value = "1@localhost:29093" },
        { name = "KAFKA_LISTENERS", value = "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9093" },
        { name = "KAFKA_ADVERTISED_LISTENERS", value = "PLAINTEXT://kafka.${var.project}.local:9092,PLAINTEXT_HOST://localhost:9093" },
        { name = "KAFKA_INTER_BROKER_LISTENER_NAME", value = "PLAINTEXT" },
        { name = "KAFKA_CONTROLLER_LISTENER_NAMES", value = "CONTROLLER" },
        { name = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", value = "1" },
        { name = "CLUSTER_ID", value = "MkU3OEVBNTcwNTJENDM2Qk" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "kafka"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "kafka" {
  name            = "${var.project}-kafka"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.kafka.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.public_subnets
    security_groups = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  service_registries {
    registry_arn = aws_service_discovery_service.kafka.arn
  }
}

# ============================================================
# FLINK JOBMANAGER
# ============================================================
resource "aws_service_discovery_service" "jobmanager" {
  name = "jobmanager"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 60
      type = "A"
    }
  }
}

resource "aws_ecs_task_definition" "jobmanager" {
  family                   = "${var.project}-jobmanager"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture       = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name  = "jobmanager"
      image = var.flink_image
      command = ["jobmanager"]
      portMappings = [
        { containerPort = 8081, hostPort = 8081 },
        { containerPort = 6123, hostPort = 6123 }
      ]
      environment = [
        { name = "KAFKA_BOOTSTRAP_SERVERS", value = "kafka.${var.project}.local:9092" },
        { name = "FLINK_PROPERTIES", value = "jobmanager.rpc.address: jobmanager.${var.project}.local\njobmanager.rpc.port: 6123" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "jobmanager"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "jobmanager" {
  name            = "${var.project}-jobmanager"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.jobmanager.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.public_subnets
    security_groups = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  service_registries {
    registry_arn = aws_service_discovery_service.jobmanager.arn
  }
}

# ============================================================
# FLINK TASKMANAGER
# ============================================================
resource "aws_ecs_task_definition" "taskmanager" {
  family                   = "${var.project}-taskmanager"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture       = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name  = "taskmanager"
      image = var.flink_image
      command = ["taskmanager"]
      environment = [
        { name = "KAFKA_BOOTSTRAP_SERVERS", value = "kafka.${var.project}.local:9092" },
        { name = "FLINK_PROPERTIES", value = "jobmanager.rpc.address: jobmanager.${var.project}.local\njobmanager.rpc.port: 6123\ntaskmanager.numberOfTaskSlots: 2" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "taskmanager"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "taskmanager" {
  name            = "${var.project}-taskmanager"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.taskmanager.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.public_subnets
    security_groups = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
}

# ============================================================
# PRODUCER
# ============================================================
resource "aws_ecs_task_definition" "producer" {
  family                   = "${var.project}-producer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture       = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name  = "producer"
      image = var.producer_image
      environment = [
        { name = "KAFKA_BOOTSTRAP_SERVERS", value = "kafka.${var.project}.local:9092" },
        { name = "KAFKA_VIEWS_TOPIC", value = "views-topic" },
        { name = "KAFKA_CLICKS_TOPIC", value = "clicks-topic" },
        { name = "PRODUCER_MODE", value = "csv" },
        { name = "PRODUCER_DELAY_MS", value = "100" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "producer"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "producer" {
  name            = "${var.project}-producer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.producer.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.public_subnets
    security_groups = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
}

# ============================================================
# CONSUMER
# ============================================================
resource "aws_ecs_task_definition" "consumer" {
  family                   = "${var.project}-consumer"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture       = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name  = "consumer"
      image = var.consumer_image
      environment = [
        { name = "KAFKA_BOOTSTRAP_SERVERS", value = "kafka.${var.project}.local:9092" },
        { name = "KAFKA_METRICS_TOPIC", value = "metrics-topic" },
        { name = "DYNAMODB_TABLE", value = "${var.project}-${var.environment}-metrics" },
        { name = "AWS_REGION", value = var.aws_region }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "consumer"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "consumer" {
  name            = "${var.project}-consumer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.consumer.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = var.public_subnets
    security_groups = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }
}

# ============================================================
# KAFKA INIT (One-time task)
# ============================================================
resource "aws_ecs_task_definition" "kafka_init" {
  family                   = "${var.project}-kafka-init"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  container_definitions = jsonencode([
    {
      name  = "kafka-init"
      image = "confluentinc/cp-kafka:7.6.0"
      entryPoint = ["/bin/bash", "-c"]
      command = [
        "kafka-topics --bootstrap-server kafka.${var.project}.local:9092 --create --if-not-exists --topic views-topic --partitions 3 --replication-factor 1 && kafka-topics --bootstrap-server kafka.${var.project}.local:9092 --create --if-not-exists --topic clicks-topic --partitions 3 --replication-factor 1 && kafka-topics --bootstrap-server kafka.${var.project}.local:9092 --create --if-not-exists --topic metrics-topic --partitions 3 --replication-factor 1"
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "kafka-init"
        }
      }
    }
  ])
}

# ============================================================
# FLINK JOB SUBMIT (One-time task)
# ============================================================
resource "aws_ecs_task_definition" "job_submit" {
  family                   = "${var.project}-job-submit"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture       = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name  = "job-submit"
      image = var.flink_image
      entryPoint = ["/bin/bash", "-c"]
      command = [
        "until curl -sf http://jobmanager.${var.project}.local:8081/overview; do sleep 5; done && /opt/flink/bin/flink run -d -m jobmanager.${var.project}.local:8081 /opt/flink/usrlib/ad-analytics.jar"
      ]
      environment = [
        { name = "KAFKA_BOOTSTRAP_SERVERS", value = "kafka.${var.project}.local:9092" },
        { name = "KAFKA_VIEWS_TOPIC", value = "views-topic" },
        { name = "KAFKA_CLICKS_TOPIC", value = "clicks-topic" },
        { name = "KAFKA_METRICS_TOPIC", value = "metrics-topic" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = var.log_group_name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "job-submit"
        }
      }
    }
  ])
}
