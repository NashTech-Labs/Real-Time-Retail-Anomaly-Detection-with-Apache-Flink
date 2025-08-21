# Start from Flink 2.1.0 image (Java 17)
FROM flink:2.1.0-scala_2.12-java17

# Set working directory inside container
WORKDIR /opt/flink

COPY flink-job/target/retail-loss-prevention-1.0-SNAPSHOT.jar /opt/flink/usrlib/
