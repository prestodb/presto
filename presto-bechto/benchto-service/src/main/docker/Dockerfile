FROM java:8
VOLUME /tmp
ADD benchto-service-1.0.0-SNAPSHOT.jar benchto-service.jar
RUN bash -c 'touch /benchto-service.jar'
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/benchto-service.jar"]
