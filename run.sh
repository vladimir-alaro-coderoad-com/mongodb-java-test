#!/bin/sh
MEM_XMX=${MEM_XMX:='6G'}
MEMORY="-Xmx$MEM_XMX -Xms$MEM_XMX"
JAVA_GC=${JAVA_GC:='-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=3 -XX:ThreadStackSize=256 -XX:MaxMetaspaceSize=128m -XX:+UseG1GC -XX:ParallelGCThreads=2 -XX:CICompilerCount=2 -XX:+UseStringDeduplication'}
JAVA_OPTS=${JAVA_OPTS:='-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=19090 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote.rmi.port=19090'}
CLUSTER_NAME=${CLUSTER_NAME:='develop'}
CLUSTER_PASSWORD=${CLUSTER_PASSWORD:='develop'}
CLUSTER_START_PORT=${CLUSTER_START_PORT:='6900'}
MC_ADDRESS=${MC_ADDRESS:='224.2.2.4'}
MC_PORT=${MC_PORT:='2904'}
java -jar -server ${MEMORY} \
                 -Dhazelcast.query.predicate.parallel.evaluation=true \
                 -Dsun.net.http.allowRestrictedHeaders=true \
                 ${JAVA_GC} \
                 ${JAVA_OPTS} \
                 payara-micro.jar \
                 --prebootcommandfile /opt/payara/pre-boot-commands.txt \
                 --clusterName ${CLUSTER_NAME} \
                 --clusterPassword ${CLUSTER_PASSWORD} \
                 --startPort ${CLUSTER_START_PORT} \
                 --mcAddress ${MC_ADDRESS} \
                 --mcPort ${MC_PORT} \
                 --addjars /opt/payara/libs/ \
                 --deploy ${DEPLOY_DIR}/mongodb-read.war
