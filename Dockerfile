FROM python:3.9.2

# Docker is the only runner we have available on Gitlab, so we have to use it on this
# non-Docker project to get Python3

COPY . /src

RUN pip install --upgrade pip && pip install -r /src/dev-requirements.txt

WORKDIR /src
