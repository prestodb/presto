# Develop and Test Prestissimo on Ubuntu

On Ubuntu 20.04 or 22.04 (other versions may work but not tested), clone the whole Presto repository and run the setup script for Prestissimo (i.e., the C++ worker):
```bash
git clone --recurse-submodules https://github.com/prestodb/presto.git
cd presto/presto-native-execution
sudo ./scripts/setup-ubuntu.sh
```

Ensure that maven (3.3.9+) and JDK8 (8u151+) are installed and in-use, they are required for building the Java codebase (coordinator and client, etc.).

## Build Java Codebase

In the root directory of the presto repository, build the Java codebase:
```bash
mvn clean install -DskipTests -T1C -pl -presto-docs
```

## Setup Development Environment in IDEs

You can optionally set up a development environment in IntelliJ and CLion.

### IntelliJ Setup

Open the presto repository as a maven project in IntelliJ.
In File->Project Structure->Project Settings->Project, choose JDK 8u151+ as the SDK, language level should be 8.
Create Run/Debug configurations in IntelliJ:
* Edit/Create `HiveQueryRunnerExternal` Application Run/Debug Configuration.
    * Main class: `com.facebook.presto.hive.HiveExternalWorkerQueryRunner`.
    * VM options: `-ea -Xmx5G -XX:+ExitOnOutOfMemoryError -Duser.timezone=America/Bahia_Banderas -Dhive.security=legacy`.
    * Working directory: `$MODULE_DIR$`
    * Environment variables: `PRESTO_SERVER=/path/to/presto/repository/presto-native-execution/cmake-build-debug/presto_cpp/main/presto_server;DATA_DIR=/path/to/data;WORKER_COUNT=0`
    * Use classpath of module: choose `presto-native-execution` module.
* Edit/Create `Presto Client` Application Run/Debug Configuration (alter paths accordingly).
    * Main class: `com.facebook.presto.cli.Presto`
    * Program arguments: `--catalog hive --schema tpch`
    * Working directory: `$MODULE_WORKING_DIR$`
    * Use classpath of module: choose `presto-cli` module.

`DATA_DIR` in the `HiveQueryRunnerExternal` Run/Debug Configuration is used to store the logs of the coordinator,
it should be an empty and writable directory for the IntelliJ user.

### CLion Setup

In CLion, create project for Presto C++ worker:
* File->Close Project if any is open.
* Open `presto/presto-native-execution` folder as CMake project and wait till CLion loads/generates cmake files, symbols, etc.
* Edit Configurations of CMake Application for `presto_server` module.
    * Program arguments: `--logtostderr=1 --v=1 --etc_dir=/path/to/etc`
    * Working directory: `/path/to/presto/repository/presto-native-execution`
* Edit menu File->Settings->Build, Execution, Deployment->CMake
    * CMake options: `-DVELOX_BUILD_TESTING=ON -DCMAKE_CXX_STANDARD=17 -DCMAKE_BUILD_TYPE=Debug`
    * Build options: `-- -j 4`

`etc_dir` in the Run/Debug Configuration of `presto_server` can be `/path/to/presto/repository/presto-native-execution/etc` 
or any other directory that contains the configuration files for the C++ worker.

### Launch in IDEs

Ensure there is enough free memory (e.g., >=12GB) in your development machine.

Run `HiveQueryRunnerExternal` from IntelliJ and wait until it started (======== SERVER STARTED ========) in the log output.
Scroll up the log output and find Discovery URL http://127.0.0.1:50555. The port is 'random' with every start.

Copy this Discovery URL to the `discovery.uri` field in `${etc_dir}/config.properties` for the worker to discover the coordinator.
And add `system-memory-gb=2` as a new line.
Set it to a larger number if you have enough memory and want to test a larger dataset.

In CLion, start the C++ worker by running `presto_server`. The worker wil automatically connector to the coordinator running in IntelliJ.
Connection success will be indicated by Announcement succeeded: 202 line in the log output.

Two ways to run Presto client to start executing queries on the running local setup:
* In command line from the root of presto repository, run the presto client:
  `java -jar presto-cli/target/presto-cli-*-executable.jar --catalog hive --schema tpch`
* Run `Presto Client` Application inside IntelliJ.

You can start from show tables; and describe table; and execute more queries as needed.

## Setup Testing Environment in Servers

In addition to the development environment in IDEs, you can test Prestissimo in servers.

### Build

In your server, after executing `setup-ubuntu.sh` script and building the Java codebase, build the C++ worker in command line:
```bash
cd /path/to/presto/repository/presto-native-execution
mkdir build-release
cd build-release
cmake -DVELOX_BUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Release ../
make presto_server -j 16
```

If you want to query Parquet tables stored on S3, turn `PRESTO_ENABLE_PARQUET` and `PRESTO_ENABLE_S3` on (around line 42-44) in
`/path/to/presto/repository/presto-native-execution/CMakeLists.txt` before running cmake.
However, the build progress will not automatically download and build the AWS SDK.
You have to build and install it following [Building the SDK](https://github.com/aws/aws-sdk-cpp#building-the-sdk).
An example for Ubuntu 20.04/22.04:
```bash
git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp
cd aws-sdk-cpp
mkdir build-release
cd build-release
sudo apt-get install libcurl4-openssl-dev -y
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_CXX_STANDARD=17 -DCMAKE_PREFIX_PATH=/usr/local/ ../
make -j 16
sudo make install
cd ../..
```
HDFS filesystem can be enabled in a similar way.

When `make presto_server` is done, the `presto_server` executable file (for the C++ worker) can be found in `/path/to/presto/repository/presto-native-execution/build-release/presto_cpp/main/`.
Also find `presto-server-0.280-SNAPSHOT.tar.gz` (for the coordinator) in `/path/to/presto/repository/presto-server/target/`.

### Install and Launch

As an example, we install the coordinator into `/opt/prestissimo/coordinator` (`${coordinator_home}`) and the worker into `/opt/prestissimo/worker` (`${worker_home}`).

Extract the content in `presto-server-0.280-SNAPSHOT.tar.gz` into `${coordinator_home}`.

Following [Presto Configuration](https://prestodb.io/docs/current/installation/deployment.html#configuring-presto)
to configure Presto coordinator (without a worker).

An example of `${coordinator_home}/etc/config.properties`:
```properties
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
query.max-memory=64GB
query.max-memory-per-node=4GB
query.max-total-memory-per-node=4GB
discovery-server.enabled=true
discovery.uri=http://localhost:8080
task.max-worker-threads=4

optimizer.optimize-hash-generation=false
parse-decimal-literals-as-double=true
experimental.internal-communication.thrift-transport-enabled=true
offset-clause-enabled=true
deprecated.legacy-date-timestamp-to-varchar-coercion=true
regex-library=RE2J
inline-sql-functions=false
use-alternative-function-signatures=true
presto.version=testversion
```

An example of `${coordinator_home}/etc/node.properties`:
```properties
node.environment=testing
node.data-dir=/opt/prestissimo/coordinator/data
node.id=coordinator
node.location=testing-location
```

Start Hive metastore and create `${coordinator_home}/etc/catalog/hive.properties` following [Hive Connector Configuration](https://prestodb.io/docs/current/connector/hive.html#configuration).
Add the following properties to `hive.properties`:
```properties
hive.pushdown-filter-enabled=true
# necessary for querying Parquet tables
hive.parquet.pushdown-filter-enabled=true
```

Ensure python is installed (to run the launch script) and the java in-use is 1.8 (8u151+), start the coordinator:
```bash
${coordinator_home}/bin/launcher start
```
Check and ensure the coordinator is started and running.

Copy `presto_server` into `${worker_home}`, create `${worker_home}/etc` directory and the following two configuration files:
* `${worker_home}/etc/config.properties`:
```properties
discovery.uri=http://localhost:8080
presto.version=testversion
http-server.http.port=7777
system-memory-gb=64
```

* `${worker_home}/etc/node.properties`:
```properties
node.environment=testing
node.id=worker1
node.ip=127.0.0.1
node.location=testing-location
```
`node.ip` must be the ipv4 address, instead of the hostname, of the server where the worker is running.
`node.environment` and `presto.version` must be consistent with the corresponding properties in `${coordinator_home}/etc/config.properties` and `${coordinator_home}/etc/node.properties`.

Copy `${coordinator_home}/etc/catalog/hive.properties` to `${worker_home}/etc/catalog/hive.properties`.
Add the following property:
```properties
hive.storage-format=DWRF
# or set it to Parquet of you want to query Parquet tables
```

Start the worker:
```bash
${worker_home}/presto_server --logtostderr=1 --v=1 --etc_dir=${worker_home}/etc &
```

If everything is correct, open http://server-hostname:8080 in browser, you should find a worker there.
After that, you can connect to the coordinator using a Presto client and run queries.
