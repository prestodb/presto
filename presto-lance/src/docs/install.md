# Installation

To run the Presto LanceDB connector, you must compile the plugin and deploy it to your Presto server's plugin directory.

## Prerequisites

- Java 17 (matching the Presto target runtime)
- Apache Maven (version 3.6 or higher)
- A running Presto Server (e.g., version 0.297 or higher)

## Build from Source

1. Clone the Presto repository containing the LanceDB connector:
   ```bash
   git clone https://github.com/prestodb/presto.git
   cd presto
   ```

2. Compile the `presto-lance` module:
   ```bash
   mvn clean package -pl presto-lance -am -DskipTests
   ```

3. Locate the compiled plugin directory in the target folder:
   ```bash
   ls presto-lance/target/presto-lance-*-SNAPSHOT/
   ```

## Deploying to Presto Server

1. Create a directory named `lance` inside the Presto server's plugin folder:
   ```bash
   mkdir -p <presto-server-root>/plugin/lance
   ```

2. Copy all JAR files from the target directory to the newly created folder:
   ```bash
   cp -r presto-lance/target/presto-lance-*-SNAPSHOT/* <presto-server-root>/plugin/lance/
   ```

3. Configure a catalog property file inside the `etc/catalog` directory to mount the connector. See [Configuration](config.md) for details.

## JVM Configuration for Java 17

Because the connector relies on Apache Arrow and vector calculations, you must configure Presto's JVM options to allow unsafe memory access and native library loading under Java 17.

Add the following options to `<presto-server-root>/etc/jvm.config`:

```ini
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

## JNI Troubleshooting

The Lance connector relies on a C++ native library (`liblance_core`) loaded via JNI:
- **Architecture Support:** The native library is compiled for common 64-bit systems (macOS x86_64, macOS aarch64/Apple Silicon, Linux x86_64, Linux aarch64, Windows x86_64).
- **Temp Directory Permissions:** The JVM extracts native libraries to the temporary directory (usually `/tmp`). If your system blocks execution in `/tmp` (e.g., mounted with the `noexec` option), the connector will fail to start and throw `java.lang.UnsatisfiedLinkError`.
  - **Resolution:** Set the JVM temporary directory to a path with executable permissions by adding this option to `etc/jvm.config`:
    ```ini
    -Djava.io.tmpdir=/path/to/executable/temp
    ```

