#!/bin/bash

#Download the PostgreSQL JDBC driver JAR file
JDBC_DRIVER_URL="https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
JDBC_DRIVER_PATH=$(mktemp)

wget -O "$JDBC_DRIVER_PATH" "JDBC_DRIVER_URL"

export JDBC_DRIVER_PATH
echo "Driver path is: $JDBC_DRIVER_PATH"