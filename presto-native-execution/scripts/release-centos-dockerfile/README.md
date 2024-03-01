# Prestissimo - Dockerfile build

> 💡 _**PrestoDB** repository: [Presto - https://github.com/prestodb/presto](https://github.com/prestodb/presto)_

> 💡 _**Velox** repository: [Velox - https://github.com/facebookincubator/velox](https://github.com/facebookincubator/velox)_

## Practical Velox implementation using PrestoCpp

> 📝 _**Note:** This readme and the build process was adapted from internal pipeline. You can e-mail the author if you've got questions [milosz.linkiewicz@intel.com](mailto:milosz.linkiewicz@intel.com)_

Prestissimo, marked in PrestoDB GitHub repository as 'presto-native-execution', is effort of making PrestoDB even better using Velox library as a starting point. Both of mentioned - PrestoCpp and Velox - are mainly written using low level `C` and `C++ 17` languages, which makes the build-from-scratch process humongously complicated. This workflow simplifies and automates Docker build process based on unmodified project GitHub repository.

## Quick Start

### 1. Clone this repository

```bash
git clone https://github.com/prestodb/presto prestodb
```

### 2. (Optional) Define and export Docker registry, image name and image tag variables

> 📝 _**Note:** Remember to end your `IMAGE_REGISTRY` with `/` as this is required for full tag generation._

> 💡 _**Tip:** Depending on your configuration you may need to run all bellow commands as root user, to switch type as your first command `sudo su`_

> 💡 _**Tip:** If `IMAGE_REGISTRY` is not specified `IMAGE_PUSH` should be set `'0'` or docker image push stage will fail._

Type in you console, changing variables values to meet your needs:

```bash
# defaults to 'avx', more info on Velox GitHub
export CPU_TARGET="avx"
# defaults to 'presto/prestissimo-${CPU_TARGET}-centos'
export IMAGE_NAME='presto/prestissimo-${CPU_TARGET}-centos'
# defaults to 'latest'
export IMAGE_TAG='latest'
# defaults to ''
export IMAGE_REGISTRY='https://my_docker_registry.com/'
# defaults to '0'
export IMAGE_PUSH='0'
```

Additionally if you are using docker registry cache for downloading images bellow values should be exported as follows:

```bash
# defaults to 'quay.io/centos/'
export IMAGE_CACHE_REGISTRY='my.company.registry.cache.xyz:5000/path/to/library/'
# this will be concatenated with (defaults to 'centos:stream8')
export IMAGE_BASE_NAME='centos:stream8'
# resulting in BASE_IMAGE:
export BASE_IMAGE="${IMAGE_CACHE_REGISTRY}${IMAGE_BASE_NAME}"
```

### 3. Make sure Docker daemon is running

(Ubuntu users) Type in your console:

```bash
systemctl status docker
```

### 4. Build Dockerfile repo

Type in your console:

```bash
cd prestodb/presto-native-execution
make runtime-container
```

The process is fully automated and require no interaction for user. The process of building images for the first time can take up to couple of hours (~1-2h using 10 processor cores).

### 5. Run container

> 📝 _**Note:** Remember that you should start Presto JAVA server first_

Depending on values you have set the container tag is defined as

`PRESTO_CPP_TAG="${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}"`

for default values this will be just:

`PRESTO_CPP_TAG=presto/prestissimo-avx-centos:latest`

to run container build with default tag execute:

```bash
docker run "presto/prestissimo-avx-centos:latest" \
            --use-env-params \
            --discovery-uri=http://localhost:8080 \
            --http-server-port=8080"
```

to run container interactively, not executing entrypoint file:

```bash
docker run -it --entrypoint=/bin/bash "presto/prestissimo-avx-centos:latest"
```

## Container manual build

For manual build outside Intel network or without access to Cloud Native Data Services Poland Docker registry follow the steps bellow.
In you terminal - with the same session that you want to build the images - define and export environment variables:

```bash
export CPU_TARGET="avx"
export IMAGE_NAME='presto/prestissimo-${CPU_TARGET}-centos'
export IMAGE_TAG='latest'
export IMAGE_REGISTRY='some-registry.my-domain.com/'
export IMAGE_PUSH='0'
export PRESTODB_REPOSITORY=$(git config --get remote.origin.url)
export PRESTODB_CHECKOUT=$(git show -s --format="%H" HEAD)
```

Where `IMAGE_NAME` and `IMAGE_TAG` will be the prestissimo release image name and tag, `IMAGE_REGISTRY` will be the registry that the image will be tagged with and which will be used to download the images from previous stages in case there are no cached images locally. The `CPU_TARGET` will be unchanged for most of the cases, for more info read the Velox documentation. The `PRESTODB_REPOSITORY` and `PRESTODB_CHECKOUT` will be used as a build repository and branch inside the container. You can set them manually or as provided using git commands.

Then to manually build the Dockerfile type:

```bash
cd presto-native-execution/scripts/release-centos-dockerfile
docker build \
    --network=host \
    --build-arg http_proxy  \
    --build-arg https_proxy \
    --build-arg no_proxy    \
    --build-arg CPU_TARGET  \
    --build-arg PRESTODB_REPOSITORY \
    --build-arg PRESTODB_CHECKOUT \
    --tag "${IMAGE_REGISTRY}${IMAGE_NAME}:${IMAGE_TAG}" .
```


## Build process - more info

> 📝 _**Note:** `prestissimo` image size is with artifacts ~35 GB, without ~10 GB_

In this step most of runtime and build time dependencies are downloaded, configured and installed. The image built in this step is a starting point for both second and third stage. It will be build 'once per breaking change' in any of repositories.
This step install Maven, Java 8, Python3-Dev, libboost-dev and lots of other massive frameworks, libraries and applications and ensures that all of steps from 2 stage will run with no errors.

Build directory structure used inside the Dockerfile:

```bash
### DIRECTORY AND MAIN BUILD ARTIFACTS
## Native Presto JAVA build artifacts:
/root/.m2/

## Build, third party dependencies, mostly for adapters
/opt/dependency/
/opt/dependency/aws-sdk-cpp
/opt/dependency/install/
/opt/dependency/install/run/
/opt/dependency/install/bin/
/opt/dependency/install/lib64/

## Root PrestoDB application directory
/opt/presto/

## Root GitHub clone of PrestoDB repository
/opt/presto/_repo/

## Root PrestoCpp subdirectory
/opt/presto/_repo/presto-native-execution/

## Root Velox GitHub repository directory, as PrestoDB submodule
/opt/presto/_repo/presto-native-execution/Velox

## Root build results directory for PrestoCpp with Velox
/opt/presto/_repo/presto-native-execution/_build/release/
/opt/presto/_repo/presto-native-execution/_build/release/velox/
/opt/presto/_repo/presto-native-execution/_build/release/presto_cpp/
```

Release image build - mostly with only the must-have runtime files, including presto_server build presto executable and some libraries. What will be used in the final released image depends on user needs and can be adjusted.

# Prestissimo - runtime configuration and settings

⚠️ _Notice: Presto-native-execution binary requires 32Gb of RAM at runtime to start (default settings). To override this and overcome runtime error add node.memory_gb=8 line in node.properties._

Presto server with all dependencies can be found inside `/opt/presto/`, runtime name is `presto_server`. There are 2 ways of starting PrestoCpp using provided entry point `/opt/entrypoint.sh`.

## 1) Quick start - pass parameters to entrypoint

This is valid when running using docker and using kubernetes. It is not advised to use this method. User should prefer mounting configuration files using Kubernetes.

```
"/opt/entrypoint.sh --use-env-params --discovery-uri=http://presto-coordinaator.default.svc.cluster.local:8080 --http-server-port=8080"
```


## 2) Using in Kubernetes environment:

Mount configuration file inside a pod as `/opt/presto/node.properties.template`. Replace each variable with preferred configuration value:

> _**Notice:** JAVA Presto Server should use same values as Presto-Native-Execution - explicitly version, location and environment should be same in both deployments - otherwise you will get connection errors._

```ini
presto.version=0.273.3
node.location=datacenter-warsaw
node.environment=test-environment
node.data-dir=/var/presto/data
catalog.config-dir=/opt/presto/catalog
plugin.dir=/opt/presto/plugin
# For nodes with less than 32Gb free memory uncomment bellow
# node.memory_gb=8
# node.id is generated and filled during machine startup if not specified
```

Mount config file inside a container as `/opt/presto/config.properties.template`. Replace each variable with your configuration values:

```ini
coordinator=false
http-server.http.port=8080
discovery.uri=http://presto-coordinaator.default.svc.cluster.local:8080
```

## 3) Hive-Metastore connector and S3 configuration:

For minimum required configuration just mount file `/opt/presto/catalog/hive.properties` inside container at give path (fill `hive.metastore.uri` with you metastore endpoint address):

```ini
connector.name=hive-hadoop2
hive.metastore.uri=thrift://hive-metastore-service.default.svc:9098
hive.pushdown-filter-enabled=true
cache.enabled=true
```

Setting required by S3 connector and Velox query engine, replace with your values, reefer to presto hive connector settings help:
```ini
hive.s3.path-style-access={{ isPathstyle }}
hive.s3.endpoint={{ scheme }}://{{ serviceFqdnTpl . }}:{{ portRest }}
hive.s3.aws-access-key={{ accessKey }}
hive.s3.aws-secret-key={{ secretKey }}
hive.s3.ssl.enabled={{ sslEnabled }}
hive.s3select-pushdown.enabled={{ s3selectPushdownFilterEnabled }}
hive.parquet.pushdown-filter-enabled={{ parquetPushdownFilterEnabled }}
```

# Appendix A - Possible flags that can be passed to runtime server
## Flags available for `/opt/presto/presto_server`:
### Flags from ./src/logging.cc:

  - alsologtoemail (log messages go to these email addresses in addition to logfiles)
    - type: string
    - default: ""
  - alsologtostderr (log messages go to stderr in addition to logfiles)
    - type: bool
    - default: false
  - colorlogtostderr (color messages logged to stderr (if supported by terminal))
    - type: bool
    - default: false
  - drop_log_memory (Drop in-memory buffers of log contents. Logs can grow very quickly and they are rarely read before they need to be evicted from memory. Instead, drop them from memory as soon as they are flushed to disk.)
    - type: bool
    - default: true
  - log_backtrace_at (Emit a backtrace when logging at file:linenum.)
    - type: string
    - default: ""
  - log_dir (If specified, logfiles are written into this directory instead of the default logging directory.)
    - type: string
    - default: ""
  - log_link (Put additional links to the log files in this directory)
    - type: string
    - default: ""
  - log_prefix (Prepend the log prefix to the start of each log line)
    - type: bool
    - default: true
  - logbuflevel (Buffer log messages logged at this level or lower (-1 means don't buffer; 0 means buffer INFO only; ...))
    - type: int32
    - default: 0
  - logbufsecs (Buffer log messages for at most this many seconds)
    - type: int32
    - default: 30
  - logemaillevel (Email log messages logged at this level or higher (0 means email all; 3 means email FATAL only; ...))
    - type: int32
    - default: 999
  - logfile_mode (Log file mode/permissions.)
    - type: int32
    - default: 436
  - logmailer (Mailer used to send logging email)
    - type: string
    - default: "/bin/mail"
  - logtostderr (log messages go to stderr instead of logfiles)
    - type: bool
    - default: false
  - max_log_size (approx. maximum log file size (in MB). A value of 0 will be silently overridden to 1.)
    - type: int32
    - default: 1800
  - minloglevel (Messages logged at a lower level than this don't actually get logged anywhere)
    - type: int32
    - default: 0
  - stderrthreshold (log messages at or above this level are copied to stderr in addition to logfiles.  This flag obsoletes --alsologtostderr.)
    - type: int32
    - default: 2
  - stop_logging_if_full_disk (Stop attempting to log to disk if the disk is full.)
    - type: bool
    - default: false

### Flags from ./src/utilities.cc:

  - symbolize_stacktrace (Symbolize the stack trace in the tombstone)
    - type: bool
    - default: true

### Flags from ./src/vlog_is_on.cc:

  - v (Show all VLOG(m) messages for m <= this. Overridable by --vmodule.)
    - type: int32
    - default: 0
  - vmodule (per-module verbose level. Argument is a comma-separated list of <module name>=<log level>. <module name> is a glob pattern, matched against the filename base (that is, name ignoring .cc/.h./-inl.h). <log level> overrides any value given by --v.)
    - type: string
    - default: ""

### Flags from /build/gflags-WDCpEz/gflags-2.2.2/src/gflags.cc:

  - flagfile (load flags from file)
    - type: string
    - default: ""
  - fromenv (set flags from the environment [use 'export FLAGS_flag1=value'])
    - type: string
    - default: ""
  - tryfromenv (set flags from the environment if present)
    - type: string
    - default: ""
  - undefok (comma-separated list of flag names that it is okay to specify on the command line even if the program does not define a flag with that name. IMPORTANT: flags in this list that have arguments MUST use the flag=value format)
    - type: string
    - default: ""

### Flags from /build/gflags-WDCpEz/gflags-2.2.2/src/gflags_completions.cc:

  - tab_completion_columns (Number of columns to use in output for tab completion)
    - type: int32
    - default: 80
  - tab_completion_word (If non-empty, HandleCommandLineCompletions() will hijack the process and attempt to do bash-style command line flag completion on this value.)
    - type: string
    - default: ""

### Flags from /build/gflags-WDCpEz/gflags-2.2.2/src/gflags_reporting.cc:

  - help (show help on all flags [tip: all flags can have two dashes])
    - type: bool
    - default: false currently: true
  - helpfull (show help on all flags -- same as -help)
    - type: bool
    - default: false
  - helpmatch (show help on modules whose name contains the specified substr)
    - type: string
    - default: ""
  - helpon (show help on the modules named by this flag value)
    - type: string
    - default: ""
  - helppackage (show help on all modules in the main package)
    - type: bool
    - default: false
  - helpshort (show help on only the main module for this program)
    - type: bool
    - default: false
  - helpxml (produce an xml version of help)
    - type: bool
    - default: false
  - version (show version and build info and exit)
    - type: bool
    - default: false

### Flags from /opt/prestodb/presto-native-execution/fbthrift/thrift/lib/cpp2/protocol/Protocol.cpp:

  - thrift_protocol_max_depth (How many nested struct/list/set/map are allowed)
    - type: int32
    - default: 15000

### Flags from /opt/prestodb/presto-native-execution/folly/folly/detail/MemoryIdler.cpp:

  - folly_memory_idler_purge_arenas (if enabled, folly memory-idler purges jemalloc arenas on thread idle)
    - type: bool
    - default: true

### Flags from /opt/prestodb/presto-native-execution/folly/folly/executors/CPUThreadPoolExecutor.cpp:

  - dynamic_cputhreadpoolexecutor (CPUThreadPoolExecutor will dynamically create and destroy threads)
    - type: bool
    - default: true

### Flags from /opt/prestodb/presto-native-execution/folly/folly/executors/IOThreadPoolExecutor.cpp:

  - dynamic_iothreadpoolexecutor (IOThreadPoolExecutor will dynamically create threads)
    - type: bool
    - default: true

### Flags from /opt/prestodb/presto-native-execution/folly/folly/executors/ThreadPoolExecutor.cpp:

  - threadtimeout_ms (Idle time before ThreadPoolExecutor threads are joined)
    - type: int64
    - default: 60000

### Flags from /opt/prestodb/presto-native-execution/folly/folly/experimental/observer/detail/ObserverManager.cpp:

  - observer_manager_pool_size (How many internal threads ObserverManager should use)
    - type: int32
    - default: 4

### Flags from /opt/prestodb/presto-native-execution/folly/folly/init/Init.cpp:

  - logging (Logging configuration)
    - type: string
    - default: ""

### Flags from /opt/prestodb/presto-native-execution/folly/folly/synchronization/Hazptr.cpp:

  - folly_hazptr_use_executor (Use an executor for hazptr asynchronous reclamation)
    - type: bool
    - default: true

### Flags from /opt/prestodb/presto-native-execution/presto_cpp/main/PrestoMain.cpp:

  - etc_dir (etc directory for presto configuration)
    - type: string
    - default: "."

### Flags from /opt/prestodb/presto-native-execution/presto_cpp/main/QueryContextManager.cpp:

  - num_query_threads (Process-wide number of query execution threads)
     - type: int32
    - default: 80

### Flags from /opt/prestodb/presto-native-execution/presto_cpp/main/TaskManager.cpp:

  - old_task_ms (Time (ms) since the task execution ended, when task is considered old.)
    - type: int32
    - default: 60000

### Flags from /opt/prestodb/presto-native-execution/proxygen/proxygen/lib/utils/ZlibStreamCompressor.cpp:

  - zlib_compressor_buffer_growth (The buffer growth size to use during IOBuf zlib deflation)
    - type: int64
    - default: 2024

### Flags from /opt/prestodb/presto-native-execution/velox/velox/common/caching/SsdFile.cpp:

  - ssd_odirect (Use O_DIRECT for SSD cache IO)
    - type: bool
    - default: true
  - ssd_verify_write (Read back data after writing to SSD)
    - type: bool
    - default: false

### Flags from /opt/prestodb/presto-native-execution/velox/velox/connectors/hive/HiveConnector.cpp:

  - file_handle_cache_mb (Amount of space for the file handle cache in mb.)
    - type: int32
    - default: 1024

### Flags from /opt/prestodb/presto-native-execution/velox/velox/dwio/common/BufferedInput.cpp:

  - wsVRLoad (Use WS VRead API to load)
    - type: bool
    - default: false

### Flags from /opt/prestodb/presto-native-execution/velox/velox/dwio/common/CachedBufferedInput.cpp:

  - cache_prefetch_min_pct (Minimum percentage of actual uses over references to a column for prefetching. No prefetch if > 100)
    - type: int32
    - default: 80

### Flags from /opt/prestodb/presto-native-execution/velox/velox/expression/Expr.cpp:

  - force_eval_simplified (Whether to overwrite queryCtx and force the use of simplified expression evaluation path.)
    - type: bool
    - default: false

### Flags from /opt/prestodb/presto-native-execution/velox/velox/flag_definitions/flags.cpp:

  - avx2 (Enables use of AVX2 when available)
    - type: bool
    - default: true
  - bmi2 (Enables use of BMI2 when available)
    - type: bool
    - default: true
  - max_block_value_set_length (Max entries per column that the block meta-record stores for pre-flight filtering optimization)
    - type: int32
    - default: 5
  - memory_usage_aggregation_interval_millis (Interval to compute aggregate memory usage for all nodes)
    - type: int32
    - default: 2
  - velox_exception_system_stacktrace_enabled (Enable the stacktrace for system type of VeloxException)
    - type: bool
    - default: true
  - velox_exception_system_stacktrace_rate_limit_ms (Min time interval in milliseconds between stack traces captured in system type of VeloxException;  off when set to 0 (the default))
    - type: int32
    - default: 0
  - velox_exception_user_stacktrace_enabled (Enable the stacktrace for user type of VeloxException)
    - type: bool
    - default: false
  - velox_exception_user_stacktrace_rate_limit_ms (Min time interval in milliseconds between stack traces captured in user type of VeloxException; off  when set to 0 (the default))
    - type: int32
    - default: 0
  - velox_memory_pool_mb (Size of file cache/operator working memory in MB)
    - type: int32
    - default: 4096
  - velox_time_allocations (Record time and volume for large allocation/free)
    - type: bool
    - default: true
  - velox_use_malloc (Use malloc for file cache and large operator allocations)
    - type: bool
    - default: true

### Flags from /opt/prestodb/presto-native-execution/wangle/wangle/ssl/SSLSessionCacheManager.cpp:

  - dcache_unit_test (All VIPs share one session cache)
    - type: bool
    - default: false