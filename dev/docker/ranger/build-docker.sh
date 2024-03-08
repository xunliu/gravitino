#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
# docker buildx build --platform linux/arm64 type=docker --progress plain -f Dockerfile -t datastrato/ranger:0.1.0 .

docker buildx build --platform linux/arm64 -t ranger:0.1.1 .

# docker run -d -it -p 6080:6080 -p 5001:5001 -v /Users/xun/github/xunliu/gravitino-test/docker/ranger:/tmp/ranger-build debian:buster bash
