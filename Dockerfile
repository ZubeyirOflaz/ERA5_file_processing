
FROM alpine:3.15


RUN apk update && apk add wget \
    && apk add python3 \
    && apk add --update py3-pip \
    && apk add git\
    && apk add cmake \
    && apk add make automake gcc g++ subversion python3-dev


WORKDIR /workspace


RUN git clone https://github.com/ZubeyirOflaz/ERA5_file_processing

WORKDIR /workspace/ERA5_file_processing

RUN pip install --upgrade pip setuptools wheel
RUN pip install --upgrade pip cython
RUN pip3 install --no-cache-dir -r requirements.txt



CMD ["sh"]