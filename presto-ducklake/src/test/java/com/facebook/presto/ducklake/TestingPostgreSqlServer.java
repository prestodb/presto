/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.ducklake;

import org.testcontainers.containers.PostgreSQLContainer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.reverseOrder;
import static java.util.Objects.requireNonNull;

/**
 * A running PostgreSQL 14 server used to host a DuckLake catalog database in tests. Two
 * implementations are available, selected with the {@code ducklake.test.postgres.mode} system
 * property: {@code testcontainers} (the default, and what CI uses) starts a {@code postgres:14}
 * Testcontainers container; {@code local} manages a scratch {@code pg_ctl} instance using the
 * PostgreSQL 14 binaries at {@code ducklake.test.pg-bin} (default {@code
 * /opt/homebrew/opt/postgresql@14/bin}), for development machines where Docker is not available.
 */
public interface TestingPostgreSqlServer
        extends Closeable
{
    String getJdbcUrl();

    String getUser();

    String getPassword();

    @Override
    void close();

    static TestingPostgreSqlServer create()
    {
        String mode = System.getProperty("ducklake.test.postgres.mode", "testcontainers");
        switch (mode) {
            case "testcontainers":
                return new TestcontainersPostgreSqlServer();
            case "local":
                String pgBinDirectory = System.getProperty("ducklake.test.pg-bin", "/opt/homebrew/opt/postgresql@14/bin");
                return new LocalPostgreSqlServer(pgBinDirectory);
            default:
                throw new IllegalArgumentException("Unknown ducklake.test.postgres.mode: " + mode);
        }
    }

    class TestcontainersPostgreSqlServer
            implements TestingPostgreSqlServer
    {
        private final PostgreSQLContainer<?> container;

        public TestcontainersPostgreSqlServer()
        {
            container = new PostgreSQLContainer<>("postgres:14")
                    .withDatabaseName("ducklake")
                    .withUsername("postgres")
                    .withPassword("ducklake");
            container.start();
        }

        @Override
        public String getJdbcUrl()
        {
            return container.getJdbcUrl();
        }

        @Override
        public String getUser()
        {
            return container.getUsername();
        }

        @Override
        public String getPassword()
        {
            return container.getPassword();
        }

        @Override
        public void close()
        {
            container.stop();
        }
    }

    class LocalPostgreSqlServer
            implements TestingPostgreSqlServer
    {
        private final Path dataDirectory;
        private final String pgBinDirectory;
        private final int port;

        public LocalPostgreSqlServer(String pgBinDirectory)
        {
            this.pgBinDirectory = requireNonNull(pgBinDirectory, "pgBinDirectory is null");
            try {
                this.dataDirectory = Files.createTempDirectory("ducklake-pg");
                this.port = findFreePort();
                run("initdb", "-D", dataDirectory.toString(), "-U", "postgres", "--auth=trust", "-E", "UTF8");
                run("pg_ctl", "-D", dataDirectory.toString(),
                        "-o", "-p " + port + " -c listen_addresses=127.0.0.1 -k " + dataDirectory,
                        "-l", dataDirectory.resolve("server.log").toString(),
                        "-w", "start");
                run("createdb", "-h", "127.0.0.1", "-p", Integer.toString(port), "-U", "postgres", "ducklake");
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static int findFreePort()
                throws IOException
        {
            try (ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            }
        }

        private void run(String... command)
                throws IOException
        {
            List<String> fullCommand = new ArrayList<>();
            fullCommand.add(Path.of(pgBinDirectory, command[0]).toString());
            for (int i = 1; i < command.length; i++) {
                fullCommand.add(command[i]);
            }

            Process process = new ProcessBuilder(fullCommand)
                    .redirectErrorStream(true)
                    .start();
            String output = new String(process.getInputStream().readAllBytes(), UTF_8);
            int exitCode;
            try {
                exitCode = process.waitFor();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for " + String.join(" ", fullCommand), e);
            }
            if (exitCode != 0) {
                throw new RuntimeException(format("Command failed with exit code %s: %s%n%s", exitCode, String.join(" ", fullCommand), output));
            }
        }

        @Override
        public String getJdbcUrl()
        {
            return "jdbc:postgresql://127.0.0.1:" + port + "/ducklake";
        }

        @Override
        public String getUser()
        {
            return "postgres";
        }

        @Override
        public String getPassword()
        {
            return "";
        }

        @Override
        public void close()
        {
            try {
                run("pg_ctl", "-D", dataDirectory.toString(), "-m", "fast", "stop");
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                deleteRecursively(dataDirectory);
            }
        }

        private static void deleteRecursively(Path directory)
        {
            try (Stream<Path> paths = Files.walk(directory)) {
                paths.sorted(reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
