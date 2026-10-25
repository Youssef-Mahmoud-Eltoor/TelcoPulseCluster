FROM apache/spark:3.5.9-scala2.12-java11-python3-r-ubuntu

USER root

COPY jars/hadoop-aws-3.3.4.jar /opt/spark/jars/
COPY jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

RUN chown spark:spark \
    /opt/spark/jars/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark
