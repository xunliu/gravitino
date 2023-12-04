# Testing ECS Exec

Run `terraform apply` to create the sample infra, and then test running a command inside the container:


```
aws ecs describe-services --cluster main --services ecs-exec | grep task

aws ecs execute-command --cluster main \
    --task a5182b7854e645ea88446de77fcf70e6 \
    --container gravitino \
    --interactive \
    --command "bash"

aws ecs execute-command --cluster main \
    --task a5182b7854e645ea88446de77fcf70e6 \
    --container hive \
    --interactive \
    --command "bash"

aws ecs execute-command --cluster main \
    --task a5182b7854e645ea88446de77fcf70e6 \
    --container trino \
    --interactive \
    --command "bash"

curl http://3.85.16.187:8090/api/metalakes



The Session Manager plugin was installed successfully. Use the AWS CLI to start a session.


Starting session with SessionId: ecs-execute-command-044e1413fda145ea9
sh-4.2# uname -a
Linux ip-172-31-4-218.eu-west-1.compute.internal 4.14.252-195.483.amzn2.x86_64 #1 SMP Mon Nov 1 20:58:46 UTC 2021 x86_64 x86_64 x86_64 GNU/Linux
sh-4.2# env
HOSTNAME=ip-172-31-45-121.eu-west-1.compute.internal
TERM=xterm-256color
AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=/v2/credentials/bbaabea1-a7d8-4071-ab34-dac0a1e7c485
AWS_EXECUTION_ENV=AWS_ECS_FARGATE
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
AWS_DEFAULT_REGION=eu-west-1
PWD=/
LANG=C.UTF-8
AWS_REGION=eu-west-1
SHLVL=1
HOME=/root
ECS_CONTAINER_METADATA_URI=http://169.254.170.2/v3/e1388fa30ec742d5aa538cf66ef9066f-4192749366
ECS_CONTAINER_METADATA_URI_V4=http://169.254.170.2/v4/e1388fa30ec742d5aa538cf66ef9066f-4192749366
_=/usr/bin/env
sh-4.2#
```
