resource "aws_ecs_cluster" "main" {
  name = "main"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_iam_role" "ecs-exec" {
  name = "ecs-exec"
  assume_role_policy = jsonencode(
    {
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Principal =  {
            Service = [
              "ecs-tasks.amazonaws.com"
            ]
          }
         Action = "sts:AssumeRole"
        }
      ]
    })

  inline_policy {
    name = "ecs_exec"
    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Effect = "Allow"
          Action = [
            "ssmmessages:CreateControlChannel",
            "ssmmessages:CreateDataChannel",
            "ssmmessages:OpenControlChannel",
            "ssmmessages:OpenDataChannel"
          ],
          Resource = "*"
        }]})
  }
}

resource "aws_ecs_task_definition" "ecs-exec" {
  family = "ecs-exec"
  task_role_arn = aws_iam_role.ecs-exec.arn
  network_mode = "awsvpc"
  requires_compatibilities = [
    "EC2",
    "FARGATE"
  ]
  cpu = "1024"
  memory = "2GB"
  container_definitions = jsonencode ([
    {
      name = "gravitino"
      image = "datastrato/gravitino:0.3.0-SNAPSHOT"
      essential = true
      portMappings: [
        {
          "containerPort": 8090,
          "hostPort": 8090
        }
      ]
      linuxParameters = {
        initProcessEnabled = true
      }
    },
    {
      name = "hive"
      image = "datastrato/hive:2.7.3-no-yarn"
      essential = true
      portMappings: [
        {
          "containerPort": 9083,
          "hostPort": 9083
        },
        {
          "containerPort": 9000,
          "hostPort": 9000
        }
      ]
      environment: [
        {
          "name": "HADOOP_USER_NAME",
          "value": "root"
        }
      ]
#      healthCheck = {
#        command = [ "CMD-SHELL", "/tmp/check-status.sh" ]
#        retries = 5
#        timeout: 60
#        interval: 10
#        startPeriod: 60
#      }
      linuxParameters = {
        initProcessEnabled = true
      }
    },
    {
      name = "trino"
      image = "datastrato/trino:aws-test" # "datastrato/trino:426-gravitino-0.3.0-SNAPSHOT"
      essential = true
      portMappings: [
        {
          "containerPort": 8080,
          "hostPort": 8080
        }
      ]
      environment: [
        {
          "name": "HADOOP_USER_NAME",
          "value": "root"
        },
        {
          "name": "GRAVITINO_HOST_IP",
          "value": "127.0.0.1"
        },
        {
          "name": "GRAVITINO_HOST_PORT",
          "value": "8090"
        },
        {
          "name": "GRAVITINO_METALAKE_NAME",
          "value": "metalake1"
        },
        {
          "name": "HIVE_HOST_IP",
          "value": "127.0.0.1"
        }
      ]
      linuxParameters = {
        initProcessEnabled = true
      }
    },
  ])
}

resource "aws_ecs_service" "ecs-exec" {
  name = "ecs-exec"
  cluster = aws_ecs_cluster.main.arn

  depends_on = [aws_iam_role.ecs-exec]

  task_definition = aws_ecs_task_definition.ecs-exec.arn
  desired_count = 1
  launch_type = "FARGATE"
  enable_execute_command = true

  network_configuration {
    subnets = data.aws_subnets.default.ids
    security_groups = [aws_security_group.ecs-exec.id]
    assign_public_ip = true
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "ecs-exec" {
  name = "ecs-exec"
  vpc_id = data.aws_vpc.default.id

  ingress {
    description = "Inbound traffic for web service"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # 允许所有来源的流量
  }

  egress {
    description = "Outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
