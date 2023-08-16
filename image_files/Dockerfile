# Use the bitnami/spark image as the base image
FROM bitnami/spark:latest

# Set the working directory
WORKDIR /app

# Copy the requirements.txt file (if you have any additional Python packages to install)
#COPY custom_nss_passwd /opt/bitnami/spark/tmp/nss_passwd
COPY requirements.txt /app

# Install the packages as root
USER root

# Install a C compiler and pip3
RUN apt-get update && \
    apt-get install -y gcc && \
    pip3 install --no-cache-dir -r requirements.txt

# Grant full permissions to non root user
RUN chmod 777 /opt/bitnami/spark/tmp
RUN chmod 777 /opt/bitnami/spark/conf
RUN chmod 777 /opt/bitnami/python/lib/python3.9/site-packages/Crypto/Cipher/XOR.py
RUN chmod 777 /app
RUN mv /app /docker_app
RUN mkdir /docker_data
RUN chmod 777 /docker_data



RUN rm /opt/bitnami/python/lib/python3.9/site-packages/Crypto/Cipher/XOR.py

COPY XOR.py /opt/bitnami/python/lib/python3.9/site-packages/Crypto/Cipher/


USER 1001
