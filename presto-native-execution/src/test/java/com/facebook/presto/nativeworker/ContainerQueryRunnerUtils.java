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
package com.facebook.presto.nativeworker;

import au.com.bytecode.opencsv.CSVReader;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ContainerQueryRunnerUtils
{
    private ContainerQueryRunnerUtils() {}

    public static void createCoordinatorTpchProperties()
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("connector.name", "tpch");
        properties.setProperty("tpch.column-naming", "STANDARD");
        createPropertiesFile("testcontainers/coordinator/etc/catalog/tpch.properties", properties);
    }

    public static void createNativeWorkerTpchProperties(String nodeId)
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("connector.name", "tpch");
        properties.setProperty("tpch.column-naming", "STANDARD");
        createPropertiesFile("testcontainers/" + nodeId + "/etc/catalog/tpch.properties", properties);
    }

    public static void createNativeWorkerConfigProperties(int coordinatorPort, String nodeId)
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("presto.version", "testversion");
        properties.setProperty("http-server.http.port", "7777");
        properties.setProperty("discovery.uri", "http://presto-coordinator:" + coordinatorPort);
        properties.setProperty("system-memory-gb", "2");
        properties.setProperty("native.sidecar", "false");
        createPropertiesFile("testcontainers/" + nodeId + "/etc/config.properties", properties);
    }

    public static void createCoordinatorConfigProperties(int port)
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("coordinator", "true");
        properties.setProperty("presto.version", "testversion");
        properties.setProperty("node-scheduler.include-coordinator", "false");
        properties.setProperty("http-server.http.port", Integer.toString(port));
        properties.setProperty("discovery-server.enabled", "true");
        properties.setProperty("discovery.uri", "http://presto-coordinator:" + port);

        // Get native worker system properties and add them to the coordinator properties
        Map<String, String> nativeWorkerProperties = NativeQueryRunnerUtils.getNativeWorkerSystemProperties();
        for (Map.Entry<String, String> entry : nativeWorkerProperties.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }

        createPropertiesFile("testcontainers/coordinator/etc/config.properties", properties);
    }

    public static void createCoordinatorJvmConfig()
            throws IOException

    {
        String jvmConfig = "-server\n" +
                "-Xmx1G\n" +
                "-XX:+UseG1GC\n" +
                "-XX:G1HeapRegionSize=32M\n" +
                "-XX:+UseGCOverheadLimit\n" +
                "-XX:+ExplicitGCInvokesConcurrent\n" +
                "-XX:+HeapDumpOnOutOfMemoryError\n" +
                "-XX:+ExitOnOutOfMemoryError\n" +
                "-Djdk.attach.allowAttachSelf=true\n";
        createScriptFile("testcontainers/coordinator/etc/jvm.config", jvmConfig);
    }

    public static void createCoordinatorLogProperties()
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("com.facebook.presto", "DEBUG");
        createPropertiesFile("testcontainers/coordinator/etc/log.properties", properties);
    }

    public static void createCoordinatorNodeProperties()
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("node.environment", "testing");
        properties.setProperty("node.location", "testing-location");
        properties.setProperty("node.data-dir", "/var/lib/presto/data");
        createPropertiesFile("testcontainers/coordinator/etc/node.properties", properties);
    }

    public static void createNativeWorkerNodeProperties(String nodeId)
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("node.environment", "testing");
        properties.setProperty("node.location", "testing-location");
        properties.setProperty("node.id", nodeId);
        properties.setProperty("node.internal-address", nodeId);
        createPropertiesFile("testcontainers/" + nodeId + "/etc/node.properties", properties);
    }

    public static void createNativeWorkerVeloxProperties(String nodeId)
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("mutable-config", "true");
        createPropertiesFile("testcontainers/" + nodeId + "/etc/velox.properties", properties);
    }

    public static void createCoordinatorEntryPointScript()
            throws IOException
    {
        String scriptContent = "#!/bin/sh\n" +
                "set -e\n" +
                "$PRESTO_HOME/bin/launcher run\n";
        createScriptFile("testcontainers/coordinator/entrypoint.sh", scriptContent);
    }

    public static void createNativeWorkerEntryPointScript(String nodeId)
            throws IOException
    {
        String scriptContent = "#!/bin/sh\n\n" +
                "GLOG_logtostderr=1 presto_server \\\n" +
                "    --etc-dir=/opt/presto-server/etc\n";
        createScriptFile("testcontainers/" + nodeId + "/entrypoint.sh", scriptContent);
    }

    public static void deleteDirectory(String directoryPath)
    {
        File directory = new File(directoryPath);
        deleteDirectory(directory);
    }

    private static void deleteDirectory(File directory)
    {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        if (!directory.delete()) {
            System.err.println("Failed to delete: " + directory.getPath());
        }
    }

    public static void createPropertiesFile(String filePath, Properties properties)
            throws IOException
    {
        File file = new File(filePath);
        File parentDir = file.getParentFile();
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }

        if (file.exists()) {
            throw new IOException("File exists: " + filePath);
        }

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)) {
            for (String key : properties.stringPropertyNames()) {
                writer.write(key + "=" + properties.getProperty(key) + "\n");
            }
        }
    }

    public static void createScriptFile(String filePath, String scriptContent)
            throws IOException
    {
        File file = new File(filePath);
        File parentDir = file.getParentFile();
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }

        if (file.exists()) {
            throw new IOException("File exists: " + filePath);
        }

        try (OutputStream output = new FileOutputStream(file);
                OutputStreamWriter writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
            writer.write(scriptContent);
        }

        file.setExecutable(true);
    }

    public static MaterializedResult toMaterializedResult(String csvData)
            throws IOException
    {
        List<List<String>> allRows = parseCsvData(csvData);

        // Infer column types based on the maximum columns found
        int maxColumns = allRows.stream().mapToInt(row -> row.size()).max().orElse(0);
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        for (int i = 0; i < maxColumns; i++) {
            final int columnIndex = i;
            columnTypesBuilder.add(inferType(allRows.stream()
                    .map(row -> columnIndex < row.size() ? row.get(columnIndex) : "")
                    .collect(Collectors.toList())));
        }
        List<Type> columnTypes = columnTypesBuilder.build();

        // Convert all rows to MaterializedRow
        ImmutableList.Builder<MaterializedRow> rowsBuilder = ImmutableList.builder();
        for (List<String> columns : allRows) {
            ImmutableList.Builder<Object> valuesBuilder = ImmutableList.builder();
            for (int i = 0; i < columnTypes.size(); i++) {
                valuesBuilder.add(i < columns.size() ? convertToType(columns.get(i), columnTypes.get(i)) : null);
            }
            rowsBuilder.add(new MaterializedRow(5, valuesBuilder.build()));
        }
        List<MaterializedRow> rows = rowsBuilder.build();

        // Create and return the MaterializedResult
        return new MaterializedResult(rows, columnTypes);
    }

    private static List<List<String>> parseCsvData(String csvData)
            throws IOException
    {
        CSVReader reader = new CSVReader(new StringReader(csvData));
        return reader.readAll().stream().map(ImmutableList::copyOf).collect(toImmutableList());
    }

    private static Type inferType(List<String> values)
    {
        boolean isBigint = true;
        boolean isDouble = true;
        boolean isBoolean = true;

        for (String value : values) {
            if (!value.matches("^-?\\d+$")) {
                isBigint = false;
            }
            if (!value.matches("^-?\\d+\\.\\d+$")) {
                isDouble = false;
            }
            if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                isBoolean = false;
            }
        }

        if (isBigint) {
            return BigintType.BIGINT;
        }
        else if (isDouble) {
            return DoubleType.DOUBLE;
        }
        else if (isBoolean) {
            return BooleanType.BOOLEAN;
        }
        else {
            return VarcharType.VARCHAR;
        }
    }

    private static Object convertToType(String value, Type type)
    {
        if (type.equals(VarcharType.VARCHAR)) {
            return value;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return Long.parseLong(value);
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return Double.parseDouble(value);
        }
        else if (type.equals(BooleanType.BOOLEAN)) {
            return Boolean.parseBoolean(value);
        }
        else {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
