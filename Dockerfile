# OS+dist should be kept in sync with .travis.yml
FROM ubuntu:focal

RUN apt-get update \
 && apt-get install -y make wget curl llvm git netcat \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libreadline-dev \
    libsqlite3-dev \
    libncurses5-dev
RUN curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash

ENV PYENV_ROOT /root/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

RUN eval "$(pyenv init -)" && eval "$(pyenv virtualenv-init -)"
RUN pyenv install 3.8.18 3.9.18 3.10.13 3.11.8 3.12.2 pypy3.10-7.3.15 \
 && pyenv local   3.8.18 3.9.18 3.10.13 3.11.8 3.12.2 pypy3.10-7.3.15

RUN pip install hatch
