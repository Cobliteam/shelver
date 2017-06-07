FROM ubuntu:16.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python-pip \
      git wget ca-certificates \
      make build-essential libssl-dev zlib1g-dev libbz2-dev \
      libreadline-dev libsqlite3-dev libncurses5-dev libffi-dev \
      tar xz-utils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ENV PYENV_ROOT="/pyenv" \
    PATH="/pyenv/bin:/pyenv/shims:$PATH"

RUN git clone --depth 1 https://github.com/yyuu/pyenv.git $PYENV_ROOT

RUN cd $PYENV_ROOT; git pull
COPY python-versions.txt /tmp
RUN xargs -P 4 -n 1 pyenv install < /tmp/python-versions.txt && \
            pyenv global $(pyenv versions --bare) && \
            find $PYENV_ROOT/versions -type d '(' -name '__pycache__' -o -name 'test' -o -name 'tests' ')' -exec rm -rfv '{}' + && \
            find $PYENV_ROOT/versions -type f '(' -name '*.py[co]' -o -name '*.exe' ')' -exec rm -fv '{}' +
RUN rm /tmp/python-versions.txt

RUN pip install -U pip
RUN pip install tox

VOLUME /app/src
WORKDIR /app/src
RUN mkdir /app/tox

RUN pip install -U tox
COPY . /app/src/
ENV TOXWORKDIR "/app/.tox"
RUN tox --notest

CMD ["tox"]
