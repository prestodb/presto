# Prestissimo - C++ Presto Worker Implementation using Velox

Prestissimo implements the [Presto Worker REST API](https://prestodb.io/docs/current/develop/worker-protocol.html)
using [Velox](https://github.com/facebookincubator/velox).

## Table of Contents
* [Build from Source](#build-from-source)
* [Build using Dockerfile](#build-using-dockerfile)
* [Development](#development)
* [Create Pull Request](#create-pull-request)
* [Advance Velox Version](#advance-velox-version)
* [Troubleshooting](#troubleshooting)

## Build from Source
* Clone the Presto repository

`git clone https://github.com/prestodb/presto.git`
* Run `cd presto/presto-native-execution && make submodules`

### Dependency installation
Dependency installation scripts based on the operating system are
available inside `presto/presto-native-execution/scripts`.

* MacOS: `setup-macos.sh`
* CentOS Stream 8: `setup-centos.sh`
* Ubuntu: `setup-ubuntu.sh`

Create a directory say `dependencies` and invoke one of these scripts from
this folder. All the dependencies are installed in the system default location eg: `/usr/local`.



The following libraries are installed by the above setup scripts.
The Velox library installs other dependencies not listed below.

| Name       | Version |
| ---------- | ------- |
| [Velox](https://github.com/facebookincubator/velox)  | Latest  |
| [CMake](https://cmake.org/) | Minimum `3.10` |
| [libsodium](https://download.libsodium.org/libsodium/releases/LATEST.tar.gz) | Latest|
| [ANTLR Runtime](https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip) |`4.9.3`|
| [protobuf](https://github.com/protocolbuffers/protobuf) |`v21.4`|
| [fizz](https://github.com/facebookincubator/fizz) |`2022.11.14.00`|
| [wangle](https://github.com/facebook/wangle) |`2022.11.14.00`|
| [proxygen](https://github.com/facebook/proxygen) |`2022.11.14.00`|
| [fbthrift](https://github.com/facebook/fbthrift) |`2022.11.14.00`|

### Build Prestissimo
To enable Parquet and S3 support, set `PRESTO_ENABLE_PARQUET = "ON"`,
`PRESTO_ENABLE_S3 = "ON"` in the environment.

S3 support needs the [AWS SDK C++](https://github.com/aws/aws-sdk-cpp) library.
This dependency can be installed by running the script below from the
`presto/presto-native-execution` directory.

`./velox/scripts/setup-adapters.sh aws`

To enable JWT authentication support, set `PRESTO_ENABLE_JWT = "ON"` in
the environment.

JWT authentication support needs the [JWT CPP](https://github.com/Thalhammer/jwt-cpp) library.
This dependency can be installed by running the script below from the
`presto/presto-native-execution` directory.

`./scripts/setup-adapters.sh jwt`

* After installing the above dependencies, from the
`presto/presto-native-execution` directory, run `make`
* For development, use
`make debug` to build a non-optimized debug version.
* Use `make unittest` to build
and run tests.

### Makefile Targets
A reminder of the available Makefile targets can be obtained using `make help`
```
    make help
    all                     Build the release version
    clean                   Delete all build artifacts
    cmake                   Use CMake to create a Makefile build system
    build                   Build the software based in BUILD_DIR and BUILD_TYPE variables
    debug                   Build with debugging symbols
    release                 Build the release version
    unittest                Build with debugging and run unit tests
    format-fix              Fix formatting issues in the current branch
    format-check            Check for formatting issues on the current branch
    header-fix              Fix license header issues in the current branch
    header-check            Check for license header issues on the current branch
    tidy-fix                Fix clang-tidy issues in the current branch
    tidy-check              Check clang-tidy issues in the current branch
    linux-container         Build the CircleCi linux container from scratch
    runtime-container       Build the software in container using current git commit
    help                    Show the help messages
```

## Build using Dockerfile
Run `make runtime-container` in the presto-native-execution root directory
to build run-ready containerized version of Prestissimo. Information on available
configuration options can be found in [scripts/release-centos-dockerfile/README.md](scripts/release-centos-dockerfile/README.md)

## Development
### Setup Presto with [IntelliJ IDEA](https://www.jetbrains.com/idea/) and Prestissimo with [CLion](https://www.jetbrains.com/clion/)

Clone the whole Presto repository. Close IntelliJ and CLion if running.

From the Presto repo run the commands below:
* `git fetch upstream`
* `git co upstream/master`
* `mvn clean install -DskipTests -T1C -pl -presto-docs`

Run IntelliJ IDEA:
* Edit/Create `HiveQueryRunnerExternal` Application Run/Debug Configuration (alter paths accordingly).
  * Main class: `com.facebook.presto.nativeworker.HiveExternalWorkerQueryRunner`.
  * VM options: `-ea -Xmx5G -XX:+ExitOnOutOfMemoryError -Duser.timezone=America/Bahia_Banderas -Dhive.security=legacy`.
  * Working directory: `$MODULE_DIR$`
  * Environment variables: `PRESTO_SERVER=/Users/<user>/git/presto/presto-native-execution/cmake-build-debug/presto_cpp/main/presto_server;DATA_DIR=/Users/<user>/Desktop/data;WORKER_COUNT=0`
  * Use classpath of module: choose `presto-native-execution` module.
* Edit/Create `TestPrestoNativeGeneralQueriesJSON` Test Run/Debug Configuration (alter paths accordingly).
  * Class: `com.facebook.presto.nativeworker.TestPrestoNativeGeneralQueriesJSON`
  * VM Options: `-ea -DPRESTO_SERVER=/Users/<user>/git/presto_cpp/cmake-build-debug/presto_cpp/main/presto_server -DDATA_DIR=/Users/<user>/Desktop/data`
  * Working directory: `$MODULE_WORKING_DIR$`
* Edit/Create `Presto Client` Application Run/Debug Configuration (alter paths accordingly).
  * Main class: `com.facebook.presto.cli.Presto`
  * Program arguments: `--catalog hive --schema tpch`
  * Use classpath of module: choose `presto-cli` module.

Run CLion:
* File->Close Project if any is open.
* Open `presto/presto-native-execution` directory as CMake project and wait till CLion loads/generates cmake files, symbols, etc.
* Edit configuration for `presto_server` module (alter paths accordingly).
  * Program arguments: `--logtostderr=1 --v=1 --etc_dir=/Users/<user>/git/presto/presto-native-execution/etc`
  * Working directory: `/Users/<user>/git/presto/presto-native-execution`
* Edit menu CLion->Preferences->Build, Execution, Deployment->CMake
  * CMake options: `-DVELOX_BUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug`
  * Build options: `-- -j 12`
  * Optional CMake options to enable Parquet and S3: `-DPRESTO_ENABLE_PARQUET=ON -DPRESTO_ENABLE_S3=ON`
* Edit menu CLion->Preferences->Editor->Code Style->C/C++
  * Scheme: `Project`
* To enable clang format you need
  * Open any h or cpp file in the editor and select `Enable ClangFormat` by clicking `4 spaces` rectangle in the status bar (bottom right) which is next to `UTF-8` bar.

    ![ScreenShot](cl_clangformat_switcherenable.png)

### Run Presto Coordinator + Worker
* Note that everything below can be done w/o using IDEs by running command line commands (not in this readme).
* Run 'HiveQueryRunnerExternal' from IntelliJ and wait until it started (`======== SERVER STARTED ========` in the log output).
* Scroll up the log output and find `Discovery URL http://127.0.0.1:50555`. The port is 'random' with every start.
* Copy that port (or the whole URL) to the `discovery.uri` field in `presto/presto-native-execution/etc/config.properties` for the worker to discover the Coordinator.
* In CLion run "presto_server" module. Connection success will be indicated by `Announcement succeeded: 202` line in the log output.
* Two ways to run Presto client to start executing queries on the running local setup:
  1. In command line from presto root directory run the presto client:
      * `java -jar presto-cli/target/presto-cli-*-executable.jar --catalog hive --schema tpch`
  2. Run `Presto Client` Application (see above on how to create and setup the configuration) inside IntelliJ
* You can start from `show tables;` and `describe table;` queries and execute more queries as needed.

### Run Integration (End to End or E2E) Tests
* Note that everything below can be done w/o using IDEs by running command line commands (not in this readme).
* Open a test file which has the test(s) you want to run in IntelliJ from `presto/presto-native-execution/src/test/java/com/facebook/presto/nativeworker` path.
* Click the green arrow to the left of the test class line of code and chose if you want to Run or Debug. This will run all tests in this class.
* Alternatively click the green arrow to the left of the test class' test method line of code and chose if you want tor Run or Debug. This will run all tests only in this class's member.
* The framework will launch single Coordinator and four native workers to test-run the queries.
* Similarly, the unit tests of Velox and presto_cpp can be run from CLion.

### Code formatting, headers, and clang-tidy

Makefile targets exist for showing, fixing and checking formatting, license
headers and clang-tidy warnings.  These targets are shortcuts for calling
`presto/presto-native-execution/scripts/check.py` .

GitHub Actions run `make format-check`, `make header-check` and
`make tidy-check` as part of our continuous integration.  Pull requests should
pass linux-build, format-check, header-check and other jobs without errors
before being accepted.

Formatting issues found on the changed lines in the current commit can be
displayed using `make format-show`.  These issues can be fixed by using `make
format-fix`.  This will apply formatting changes to changed lines in the
current commit.

Header issues found on the changed files in the current commit can be displayed
using `make header-show`.  These issues can be fixed by using `make
header-fix`.  This will apply license header updates to the files in the current
commit.

Similar commands `make tidy-show`, `make-tidy-fix`, `make tidy-check` exist for
running clang-tidy, but these checks are currently advisory only.

An entire directory tree of files can be formatted and have license headers added
using the `tree` variant of the format.sh commands:
```
presto/presto-native-execution/scripts/check.py format tree
presto/presto-native-execution/scripts/check.py format tree --fix

presto/presto-native-execution/scripts/check.py header tree
presto/presto-native-execution/scripts/check.py header tree --fix
```

All the available formatting commands can be displayed by using
`presto/presto-native-execution/scripts/check.py help`.

There is currently no mechanism to *opt out* files or directories from the
checks.  When we need one it can be added.

## Create Pull Request
* Submit PRs as usual following [Presto repository guidelines](https://github.com/prestodb/presto/wiki/Review-and-Commit-guidelines).
* Prestissimo follows the Velox [coding style](https://github.com/facebookincubator/velox/blob/main/CODING_STYLE.md).
* Add `[native]` prefix in the `title` as well as to the `commit message` for PRs modifying anything in `presto-native-execution`.
* PRs that only change files in `presto-native-execution` should be approved by a Code Owner ([team-velox](https://github.com/orgs/prestodb/teams/team-velox)) to have merging enabled.

## Advance Velox Version
For Prestissimo to use a newer Velox version from the Presto repository root:
* `git -C presto-native-execution/velox checkout main`
* `git -C presto-native-execution/velox pull`
* `git add presto-native-execution/velox`
* Build and run tests (including E2E) to ensure everything works.
* Submit a PR, get it approved and merged.

## Troubleshooting
For known build issues check the wiki page [Troubleshooting known build issues](https://github.com/prestodb/presto/wiki/Troubleshooting-known-build-issues).
