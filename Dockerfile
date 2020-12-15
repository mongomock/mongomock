# OS+dist should be kept in sync with .travis.yml
FROM ubuntu:xenial

RUN apt-get update && apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev git netcat
RUN curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash

ENV PYENV_ROOT /root/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

RUN eval "$(pyenv init -)" && eval "$(pyenv virtualenv-init -)"
RUN pyenv install 2.7.18
RUN pyenv install 3.4.10
RUN pyenv install 3.5.9
RUN pyenv install 3.6.11
RUN pyenv install 3.7.8
RUN pyenv install 3.8.5
RUN pyenv install 3.9.0
RUN pyenv install pypy2.7-7.1.1
RUN pyenv local 2.7.18 3.4.10 3.5.9 3.6.11 3.7.8 3.8.5 3.9.0 pypy2.7-7.1.1

RUN pip install tox
