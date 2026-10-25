FROM telcopulse-spark-s3a:3.5.9
USER root
RUN /usr/bin/python3 -m pip install --no-cache-dir "numpy<2"
USER spark
