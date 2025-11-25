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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UnknownType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

    public static void createCoordinatorTpcdsProperties()
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("connector.name", "tpcds");
        properties.setProperty("tpcds.use-varchar-type", "true");
        createPropertiesFile("testcontainers/coordinator/etc/catalog/tpcds.properties", properties);
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

        try (OutputStream output = new FileOutputStream(file);
                OutputStreamWriter writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
            writer.write(scriptContent);
        }

        file.setExecutable(true);
    }

    public static MaterializedResult toMaterializedResult(ResultSet resultSet)
            throws SQLException
    {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // List to hold the column types
        List<Type> types = new ArrayList<>();

        // Map SQL types to Presto types
        for (int i = 1; i <= columnCount; i++) {
            int sqlType = metaData.getColumnType(i);
            String typeName = metaData.getColumnTypeName(i);

            if (sqlType == java.sql.Types.ARRAY) {
                TypeSignature typeSignature = TypeSignature.parseTypeSignature(typeName);
                List<TypeSignatureParameter> parameters = typeSignature.getParameters();

                if (parameters.size() == 1) {
                    TypeSignature baseTypeSignature = parameters.get(0).getTypeSignature();
                    Type elementType = mapSqlTypeNameToType(baseTypeSignature.toString());
                    types.add(new ArrayType(elementType));
                }
                else {
                    throw new UnsupportedOperationException("Unsupported ARRAY type with multiple parameters: " + typeName);
                }
            }
            else if (sqlType == java.sql.Types.STRUCT) {
                // Handle STRUCT types if necessary
                types.add(UnknownType.UNKNOWN);
            }
            else {
                types.add(mapSqlTypeToType(sqlType, typeName));
            }
        }

        // List to store the rows
        List<MaterializedRow> rows = new ArrayList<>();

        // Iterate over the ResultSet and convert each row to a MaterializedRow
        while (resultSet.next()) {
            List<Object> rowData = new ArrayList<>();

            for (int i = 1; i <= columnCount; i++) {
                Object value;
                int sqlType = metaData.getColumnType(i);

                if (sqlType == java.sql.Types.ARRAY) {
                    java.sql.Array array = resultSet.getArray(i);
                    if (array != null) {
                        Object[] javaArray = (Object[]) array.getArray();
                        // Recursively convert array elements if necessary
                        value = Arrays.asList(javaArray);
                    }
                    else {
                        value = null;
                    }
                }
                else if (sqlType == java.sql.Types.STRUCT) {
                    // Handle STRUCT types if necessary
                    value = resultSet.getObject(i);
                }
                else {
                    value = resultSet.getObject(i);
                }
                rowData.add(value);
            }

            // Use the default precision from MaterializedResult
            rows.add(new MaterializedRow(MaterializedResult.DEFAULT_PRECISION, rowData));
        }

        // Return the MaterializedResult
        return new MaterializedResult(rows, types);
    }

    private static Type mapSqlTypeToType(int sqlType, String typeName)
    {
        switch (sqlType) {
            case java.sql.Types.INTEGER:
                return IntegerType.INTEGER;
            case java.sql.Types.BIGINT:
                return BigintType.BIGINT;
            case java.sql.Types.SMALLINT:
                return SmallintType.SMALLINT;
            case java.sql.Types.TINYINT:
                return TinyintType.TINYINT;
            case java.sql.Types.DOUBLE:
                return DoubleType.DOUBLE;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                return RealType.REAL;
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                return DecimalType.createDecimalType();
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CHAR:
                return VarcharType.createVarcharType(1);
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return BooleanType.BOOLEAN;
            case java.sql.Types.DATE:
                return DateType.DATE;
            case java.sql.Types.TIME:
                return TimeType.TIME;
            case java.sql.Types.TIMESTAMP:
                return TimestampType.TIMESTAMP;
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return VarbinaryType.VARBINARY;
            case java.sql.Types.OTHER:
                // Attempt to map based on type name
                return mapSqlTypeNameToType(typeName);
            default:
                throw new UnsupportedOperationException("Unknown SQL type: " + sqlType + ", TypeName: " + typeName);
        }
    }

    private static Type mapSqlTypeNameToType(String typeName)
    {
        switch (typeName.toUpperCase()) {
            case "INT":
            case "INTEGER":
            case "INT4":
                return IntegerType.INTEGER;
            case "BIGINT":
            case "INT8":
                return BigintType.BIGINT;
            case "SMALLINT":
                return SmallintType.SMALLINT;
            case "TINYINT":
                return TinyintType.TINYINT;
            case "FLOAT":
            case "REAL":
                return RealType.REAL;
            case "DOUBLE":
            case "FLOAT8":
                return DoubleType.DOUBLE;
            case "DECIMAL":
            case "NUMERIC":
                return DecimalType.createDecimalType();
            case "VARCHAR":
            case "TEXT":
            case "STRING":
                return VarcharType.createUnboundedVarcharType();
            case "CHAR":
                return CharType.createCharType(1);
            case "BOOLEAN":
                return BooleanType.BOOLEAN;
            case "DATE":
                return DateType.DATE;
            case "TIME":
                return TimeType.TIME;
            case "TIMESTAMP":
                return TimestampType.TIMESTAMP;
            case "TIMESTAMP WITH TIME ZONE":
                return TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
            case "VARBINARY":
            case "BINARY":
                return VarbinaryType.VARBINARY;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + typeName.toUpperCase());
        }
    }
}
