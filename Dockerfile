FROM ubuntu:noble
RUN apt-get update \
 && apt-get install -y \
    curl \
    git \
    llvm \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libreadline-dev \
    libsqlite3-dev \
    libncurses5-dev \
    pipx

ENV PATH=/root/.local/bin:${PATH}
RUN pipx ensurepath && pipx install hatch
RUN hatch python install 3.9 3.10 3.11 3.12 3.13 pypy3.10
