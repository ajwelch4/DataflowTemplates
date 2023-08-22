FROM apache/beam_java11_sdk:2.45.0

# DVT

RUN /bin/bash -c 'set -x \
    && cd /opt \
    && apt-get update \
    && apt-get install -y git python3 python3-venv \
    && mkdir -p ~/.ssh \
    && ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts \
    && git clone https://github.com/GoogleCloudPlatform/professional-services-data-validator.git \
    && cd professional-services-data-validator \
    && python3 -m venv env \
    && source env/bin/activate \
    && python3 -m pip install .'
