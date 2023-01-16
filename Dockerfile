
FROM ubuntu:22.10


RUN apt-get update && apt-get install -y wget \
    && apt-get install -y python3 \
    && apt-get install -y python3-pip \
    && apt-get install -y git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace


RUN git clone https://github.com/ZubeyirOflaz/ERA5_file_processing

WORKDIR /workspace/ERA5_file_processing


RUN pip3 install --no-cache-dir -r requirements.txt



CMD ["bash"]