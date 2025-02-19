# Use the bitnami/spark image as the base image
FROM bitnami/spark:3.3.2-debian-11-r18

# Set the working directory
WORKDIR /app

# Copy the requirements.txt file (if you have any additional Python packages to install)
#COPY custom_nss_passwd /opt/bitnami/spark/tmp/nss_passwd
COPY requirements.txt /app

# Install the packages as root
USER root

# Install a C compiler and pip3
# Install dependencies and libraries
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y gcc unixodbc unixodbc-dev wget && \
    pip3 install --no-cache-dir -r requirements.txt


# Grant full permissions to non root user
RUN chmod 777 /opt/bitnami/spark/tmp \
              /opt/bitnami/spark/conf \
              /opt/bitnami/spark/jars \
              /app

RUN chmod 777 /opt/bitnami/python/lib/python3.9/site-packages/Crypto/Cipher/XOR.py
RUN mv /app /docker_app
RUN mkdir /docker_data
RUN chmod 777 /docker_data

ENV PYARROW_IGNORE_TIMEZONE=1

# Replace the XOR.py script with working version
RUN rm /opt/bitnami/python/lib/python3.9/site-packages/Crypto/Cipher/XOR.py 
COPY XOR.py /opt/bitnami/python/lib/python3.9/site-packages/Crypto/Cipher/


###### Snowflake Connector ######

# Download the correct Spark-Snowflake connector (Scala 2.12, Spark 3.3)
RUN wget -O /opt/bitnami/spark/jars/spark-snowflake_2.12-2.16.0-spark_3.3.jar \
    https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.16.0-spark_3.3/spark-snowflake_2.12-2.16.0-spark_3.3.jar

# Download the latest Snowflake JDBC driver
RUN wget -O /opt/bitnami/spark/jars/snowflake-jdbc.jar \
    https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.2/snowflake-jdbc-3.14.2.jar

# Ensure JAR files have the correct permissions
RUN chmod 644 /opt/bitnami/spark/jars/*.jar

# Verify JAR files exist
RUN ls -lah /opt/bitnami/spark/jars/

# Set user back to non-root
USER 1001
