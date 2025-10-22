FROM python:3.10

RUN apt-get update \
    && apt-get install -y git pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install git+https://github.com/mancusolab/sushie.git
