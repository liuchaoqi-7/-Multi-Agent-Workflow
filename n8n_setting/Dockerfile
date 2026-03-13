# 基于 n8n 中文版
# FROM deluxebear/n8n:2.6.4-chs
FROM n8nio/n8n:nightly-arm64
USER root

# 更换为阿里云 Alpine 镜像源，并安装 Python 及编译依赖
# RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories && \
#     apk add --no-cache \
#     python3 \
#     py3-pip \
#     mariadb-client \
#     python3-dev \
#     py3-virtualenv \
#     build-base

# 使用清华源安装 Python 库，避免 pip 卡死
# RUN pip3 install --break-system-packages -i https://pypi.tuna.tsinghua.edu.cn/simple \
#     pandas requests sqlalchemy pymysql tqdm

USER node