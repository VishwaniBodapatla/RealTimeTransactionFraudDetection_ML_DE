# Use the official Python 3.10 slim image based on Debian Bookworm

From python:3.10.16-slim-bookworm

# Set environment variables
# - DEBIAN_FRONTEND=noninteractive: prevents interactive prompts during package installs
# - PYTHONUNBUFFERED=1: forces Python to print output immediately (no buffering)
# - PYTHONIOENCODING=UTF-8: ensures Python handles all characters (UTF-8) correctly
ENV DEBIAN_FRONTEND=noninteractive \ 
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

# Set working directory inside the container
WORKDIR /app


# Install necessary system packages:
# This command installs everything your container needs to compile C/C++ libraries (like librdkafka) 
# and support secure Kafka connections, then cleans up to keep the image small.
RUN  apt-get update && \
     apt-get install -y --no-install-recommends \
     curl \
     gnupg \
     gcc \
     g++ \
     build-essential \
     python3-dev \
     libssl-dev \
     libsasl2-dev \
     pkg-config \
     && rm -rf /var/lib/apt/lists/*


# Manually install librdkafka with support from source with SSL/SASL enabled
# This block downloads, builds, and installs librdkafka with security features, then cleans up the mess to keep the image short.
RUN curl -L https://github.com/confluentinc/librdkafka/archive/refs/tags/v2.5.0.tar.gz -o librdkafka.tar.gz && \
    tar -xzf librdkafka.tar.gz && \
    cd librdkafka-2.5.0 && \
    ./configure --enable-ssl --enable-sasl && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd .. && rm -rf librdkafka.tar.gz librdkafka-2.5.0


COPY ./requirements.txt /app/requirements.txt


RUN pip install -r /app/requirements.txt


COPY . .

ENTRYPOINT ["python3", "Transaction_data_Producer.py"]
