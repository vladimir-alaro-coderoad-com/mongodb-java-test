FROM mojix/base-tomcat:mongo3.6 as builder
WORKDIR /opt
RUN yum install wget -y

# Docker best practices advice against using ADD to download files, use copy instead
# by using a single RUN command we save up on the number of images created
RUN mkdir -p /opt/libs && cd /opt/libs \
    && wget http://central.maven.org/maven2/com/google/http-client/google-http-client/1.20.0/google-http-client-1.20.0.jar \
    && wget http://central.maven.org/maven2/com/google/http-client/google-http-client-xml/1.20.0/google-http-client-xml-1.20.0.jar \
    && wget http://central.maven.org/maven2/com/squareup/okhttp3/okhttp/3.7.0/okhttp-3.7.0.jar \
    && wget http://central.maven.org/maven2/com/squareup/okio/okio/1.12.0/okio-1.12.0.jar \
    && wget http://central.maven.org/maven2/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar \
    && wget http://central.maven.org/maven2/org/mongodb/mongodb-driver/3.10.0/mongodb-driver-3.10.0.jar \
    && wget http://central.maven.org/maven2/org/mongodb/mongodb-driver-async/3.10.0/mongodb-driver-async-3.10.0.jar \
    && wget http://central.maven.org/maven2/org/mongodb/mongodb-driver-reactivestreams/1.11.0/mongodb-driver-reactivestreams-1.11.0.jar \
    && wget http://central.maven.org/maven2/org/mongodb/mongodb-driver-core/3.10.0/mongodb-driver-core-3.10.0.jar \
    && wget http://central.maven.org/maven2/org/mongodb/bson/3.10.0/bson-3.10.0.jar \
    && wget http://central.maven.org/maven2/org/reactivestreams/reactive-streams/1.0.2/reactive-streams-1.0.2.jar \
    \
    && chmod +r -R /opt/libs/*

# finally copy the actual source files
COPY mongodb-read/ /opt/mongodb-read/
COPY pre-boot-commands.txt /opt/pre-boot-commands.txt
COPY run.sh /opt/run.sh
RUN chmod +x /opt/run.sh

FROM payara/micro:5.183
USER root
RUN apk add --no-cache curl
COPY --from=builder /opt/libs/* /opt/payara/libs/
COPY --from=builder /opt/pre-boot-commands.txt /opt/payara/pre-boot-commands.txt
COPY --from=builder /opt/mongodb-read/target/mongodb-read.war $DEPLOY_DIR
COPY --from=builder /opt/run.sh /opt/run.sh
ENTRYPOINT ["/opt/run.sh"]