FROM themattrix/tox

RUN apt-get update && apt-get install -y --no-install-recommends git-core tar xz-utils
