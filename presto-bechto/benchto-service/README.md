# Benchto-service

Service for storing/showing benchmark results.

## Prerequisites

* Java 8

* PostgreSQL:

For local development use docker:

```
$ docker run --name benchto-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
```

## Running integration-tests

```
$ mvn verify
```

## Running service

```
$ mvn spring-boot:run

------------------------------------------------------------------

      Copyright 2013-2015, Teradata, Inc. All rights reserved.

               Benchto-service  (v1.0.0-SNAPSHOT)

------------------------------------------------------------------

11:22:30.170 INFO  com.teradata.benchmark.service.App - Starting App v1.0.0-SNAPSHOT on latitude with PID 8659 (/home/sogorkis/repos/benchmark-service/target/service-1.0.0-SNAPSHOT.jar started by sogorkis in /home/sogorkis/repos/benchmark-service)
...
```

Go to: [http://localhost:8080/](http://localhost:8080/)

## Creating environment

To create environment PRESTO-DEVENV you need to run:

```
$ curl -H 'Content-Type: application/json' -d '{
    "dashboardType": "grafana",
    "dashboardURL": "http://localhost:3000/dashboard/db/presto-devenv",
    "prestoURL": "http://presto-master:8080/"
}' http://localhost:8080/v1/environment/PRESTO-DEVENV
```

## Creating tag

To create tag for environment PRESTO-DEVENV you need to run:

```
$ curl -H 'Content-Type: application/json' -d '{
    "name": "Short tag desciption",
    "description": "Very long but optional tag description"
}' http://localhost:8080/v1/tag/PRESTO-DEVENV

```

Note that `description` field is optional.

## Building docker image

```
$ mvn docker:build
$ docker images
REPOSITORY                             TAG                 IMAGE ID            CREATED             VIRTUAL SIZE
teradata-labs/benchto-service          latest              427f3e1f4777        13 seconds ago      879.3 MB
...
$ docker save teradata-labs/benchto-service | gzip > /tmp/benchto-service.tar.gz
```

## Cleaning up stale benchmark runs

Benchmark runs that are older then 24 hours and have not finished will be periodically cleaned up and automatically failed.
