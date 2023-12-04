terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "4.45.0"
    }
  }
}

provider "aws" {
  region  = "us-east-1" # The region where environment is going to be deployed # Use your own region here
  access_key = "AKIAZ6VQTBSXRRTNJEXQ" # Enter AWS IAM
  secret_key = "xmmwaWR+emndPrEIePUVSHTAm6vYYmH/+dE8Z/2Z" # Enter AWS IAM
}


# resource "aws_ecr_repository" "app_ecr_repo" {
#   name = "app-repo"
# }

data "aws_ecr_repository" "app_ecr_repo" {
  name = "gravitino"
}

data "aws_ecr_repository" "hive_ecr_repo" {
  name = "hive"
}

resource "aws_ecs_cluster" "my_cluster" {
  name = "app-cluster" # Name your cluster here
}

resource "aws_ecs_task_definition" "app_task" {
  family                   = "app-first-task" # Name your task
  container_definitions    = <<DEFINITION
  [
    {
      "name": "app-first-task",
      "image": "${data.aws_ecr_repository.app_ecr_repo.repository_url}:0.3.0-SNAPSHOT",
      "essential": true,
      "portMappings": [
        {
          "containerPort": 8090,
          "hostPort": 8090
        }
      ],
      "memory": 256,
      "cpu": 128
    },
    {
      "name": "hive-task",
      "image": "${data.aws_ecr_repository.hive_ecr_repo.repository_url}:2.7.3-no-yarn",
      "essential": true,
      "portMappings": [
        {
          "containerPort": 9083,
          "hostPort": 9083
        },
        {
          "containerPort": 9000,
          "hostPort": 9000
        }
      ],
      "memory": 256,
      "cpu": 128
    }
  ]
  DEFINITION
  requires_compatibilities = ["FARGATE"] # use Fargate as the launch type
  network_mode             = "awsvpc"    # add the awsvpc network mode as this is required for Fargate
  memory                   = 512         # Specify the memory the container requires
  cpu                      = 256         # Specify the CPU the container requires
  execution_role_arn       = "${aws_iam_role.ecsTaskExecutionRole.arn}"
}

#resource "aws_iam_role" "ecs-exec" {
#  name = "ecs-exec"
#  assume_role_policy = jsonencode(
#    {
#      Version = "2012-10-17"
#      Statement = [
#        {
#          Effect = "Allow"
#          Principal =  {
#            Service = [
#              "ecs-tasks.amazonaws.com"
#            ]
#          }
#          Action = "sts:AssumeRole"
#        }
#      ]
#    })
#
#  inline_policy {
#    name = "ecs_exec"
#    policy = jsonencode({
#      Version = "2012-10-17"
#      Statement = [
#        {
#          Effect = "Allow"
#          Action = [
#            "ssmmessages:CreateControlChannel",
#            "ssmmessages:CreateDataChannel",
#            "ssmmessages:OpenControlChannel",
#            "ssmmessages:OpenDataChannel"
#          ],
#          Resource = "*"
#        }]})
#  }
#}

resource "aws_iam_role" "ecsTaskExecutionRole" {
  name = "ecs_combined"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect    = "Allow",
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        },
        Action    = "sts:AssumeRole"
      }
    ]
  })

  inline_policy {
    name   = "ecs_exec"
    policy = jsonencode({
      Version   = "2012-10-17",
      Statement = [
        {
          Effect   = "Allow",
          Action   = [
            "ssmmessages:CreateControlChannel",
            "ssmmessages:CreateDataChannel",
            "ssmmessages:OpenControlChannel",
            "ssmmessages:OpenDataChannel"
          ],
          Resource = "*"
        }
      ]
    })
  }
}

#resource "aws_iam_role" "ecsTaskExecutionRole" {
#  name               = "ecsTaskExecutionRole"
#  assume_role_policy = "${data.aws_iam_policy_document.assume_role_policy.json}"
#}
#
#data "aws_iam_policy_document" "assume_role_policy" {
#  statement {
#    actions = ["sts:AssumeRole"]
#
#    principals {
#      type        = "Service"
#      identifiers = ["ecs-tasks.amazonaws.com"]
#    }
#  }
#}

resource "aws_iam_role_policy_attachment" "ecsTaskExecutionRole_policy" {
  role       = "${aws_iam_role.ecsTaskExecutionRole.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}


# Provide a reference to your default VPC
resource "aws_default_vpc" "default_vpc" {
}

# Provide references to your default subnets
resource "aws_default_subnet" "default_subnet_a" {
  # Use your own region here but reference to subnet 1a
  availability_zone = "us-east-1a"
}

resource "aws_default_subnet" "default_subnet_b" {
  # Use your own region here but reference to subnet 1b
  availability_zone = "us-east-1b"
}

resource "aws_alb" "application_load_balancer" {
  name               = "load-balancer-dev" # Naming our load balancer
  load_balancer_type = "application"
  subnets = [ # Referencing the default subnets
    "${aws_default_subnet.default_subnet_a.id}",
    "${aws_default_subnet.default_subnet_b.id}"
  ]
  # Referencing the security group
  security_groups = ["${aws_security_group.load_balancer_security_group.id}"]
}

# Creating a security group for the load balancer:
resource "aws_security_group" "load_balancer_security_group" {
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Allowing traffic in from all sources
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_lb_target_group" "target_group" {
  name        = "target-group"
  port        = 80
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = "${aws_default_vpc.default_vpc.id}" # Referencing the default VPC
}

resource "aws_lb_listener" "listener" {
  load_balancer_arn = "${aws_alb.application_load_balancer.arn}" # Referencing our load balancer
  port              = "80"
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = "${aws_lb_target_group.target_group.arn}" # Referencing our tagrte group
  }
}

resource "aws_ecs_service" "app_service" {
  name            = "app-first-service"                             # Name the  service
  cluster         = "${aws_ecs_cluster.my_cluster.id}"             # Reference the created Cluster

  depends_on = [aws_iam_role.ecsTaskExecutionRole]

  task_definition = "${aws_ecs_task_definition.app_task.arn}" # Reference the task that the service will spin up
  launch_type     = "FARGATE"
  enable_execute_command = true
  desired_count   = 1 # Set up the number of containers to 3

  load_balancer {
    target_group_arn = "${aws_lb_target_group.target_group.arn}" # Reference the target group
    container_name   = "${aws_ecs_task_definition.app_task.family}"
    container_port   = 8090 # Specify the container port
  }

  network_configuration {
    subnets          = ["${aws_default_subnet.default_subnet_a.id}", "${aws_default_subnet.default_subnet_b.id}"]
    assign_public_ip = true                                                # Provide the containers with public IPs
    security_groups  = ["${aws_security_group.service_security_group.id}"] # Set up the security group
  }
}

resource "aws_security_group" "service_security_group" {
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    # Only allowing traffic in from the load balancer security group
    security_groups = ["${aws_security_group.load_balancer_security_group.id}"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

#Log the load balancer app url
output "app_url" {
  value = aws_alb.application_load_balancer.dns_name
}
