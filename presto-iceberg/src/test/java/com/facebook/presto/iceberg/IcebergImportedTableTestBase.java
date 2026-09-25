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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.write;

public abstract class IcebergImportedTableTestBase
        extends AbstractTestQueryFramework
{
    protected static final String CATALOGNAME = "iceberg";
    protected static final String SCHEMANAME = "iceberg_v3";

    private static final String METADATA = "metadata";
    private static final String PATHPLACEHOLDER = "FILEPATH";

    protected abstract QueryRunner createQueryRunner() throws Exception;

    @BeforeClass
    public void createSchema()
    {
        QueryRunner queryRunner = getQueryRunner();
        if (queryRunner != null) {
            createSchema(CATALOGNAME, SCHEMANAME, queryRunner);
        }
    }

    @AfterClass
    public void deleteSchema()
    {
        QueryRunner queryRunner = getQueryRunner();
        if (queryRunner != null) {
            dropSchema(CATALOGNAME, SCHEMANAME, queryRunner);
        }
    }

    private static void createSchema(String catalog, String schema, QueryRunner queryRunner)
    {
        if (queryRunner != null) {
            queryRunner.execute(format(
                    "CREATE SCHEMA IF NOT EXISTS %s.%s",
                    catalog,
                    schema));
        }
    }

    private static void dropSchema(String catalog, String schema, QueryRunner queryRunner)
    {
        if (queryRunner != null) {
            queryRunner.execute(format(
                    "DROP SCHEMA IF EXISTS %s.%s",
                    catalog,
                    schema));
        }
    }

    protected String setupAndRegisterTable(String testName)
    {
        String tablePath = goldenTablePathWithPrefix(SCHEMANAME, testName);

        // Create temp directory
        File tempDirectory = null;
        String activeTableParent = null;

        try {
            tempDirectory = createTempDirectory("IcebergTemporaryTable").toFile();
            File tempTable = new File(tempDirectory, testName);
            FileUtils.copyDirectory(new File(tablePath), tempTable);

            // Save temp directory and table
            activeTableParent = tempDirectory.getAbsolutePath();
            String activeTable = tempTable.getAbsolutePath();

            File tempMetadata = new File(tempTable, METADATA);

            if (!tempMetadata.isDirectory()) {
                throw new RuntimeException("Metadata folder does not exist in iceberg table at: " + tempDirectory);
            }

            // Update all .avro files
            File[] avroFiles = tempMetadata.listFiles((dir, name) -> name.endsWith(".avro"));
            for (File avroFile : avroFiles) {
                List<String> jsonRecords;
                Schema schema;

                // Load avro files
                try (DataFileReader<GenericRecord> reader = new DataFileReader<>(avroFile, new GenericDatumReader<>())) {
                    // Get avro file schema
                    schema = reader.getSchema();

                    // Convert avro to json
                    jsonRecords = avroToJson(reader, schema);
                }

                // String replace
                List<String> updatedJsonRecords = new ArrayList<>();
                for (String record : jsonRecords) {
                    updatedJsonRecords.add(record.replace(PATHPLACEHOLDER, activeTable));
                }

                // Convert json to avro and update existing avroFile
                jsonToAvro(updatedJsonRecords, avroFile.getAbsolutePath(), schema);
            }

            // Update all .metadata.json files
            File[] jsonFiles = tempMetadata.listFiles((dir, name) -> name.endsWith(".json"));
            for (File jsonFile : jsonFiles) {
                // Use java Files to read file
                String fileContent = new String(readAllBytes(jsonFile.toPath()), java.nio.charset.StandardCharsets.UTF_8);
                // Replace the placeholder with absolute path
                fileContent = fileContent.replace(PATHPLACEHOLDER, activeTable);
                // Write json back to file
                write(jsonFile.toPath(), fileContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }

            // Add table to schema
            Session session = Session.builder(getSession()).build();
            String queryCreate = format("CALL %s.system.register_table( " +
                    "schema => '%s', " +
                    "table_name => '%s', " +
                    "metadata_location => 'file://%s'" +
                    ")", CATALOGNAME, SCHEMANAME, testName, activeTable);
            computeActual(session, queryCreate);

            return activeTableParent;
        }

        catch (Exception e) {
            // Delete temp directory
            dropAndCleanupTable(testName, activeTableParent);
            throw new RuntimeException(e);
        }
    }

    protected void dropAndCleanupTable(String testName, String activeTableDirectory)
    {
        // Remove table from schema
        Session session = Session.builder(getSession()).build();
        String queryDrop = format("DROP TABLE IF EXISTS %s.%s", SCHEMANAME, testName);
        computeActual(session, queryDrop);

        // Delete table from temp directory
        if (activeTableDirectory != null) {
            try {
                FileUtils.deleteDirectory(new File(activeTableDirectory));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void jsonToAvro(List<String> json, String avroAbsolutePath, Schema schema)
    {
        try (DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            fileWriter.create(schema, new File(avroAbsolutePath));

            for (String jsonRecord : json) {
                JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonRecord);
                GenericRecord updated = datumReader.read(null, decoder);
                fileWriter.append(updated);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> avroToJson(DataFileReader<GenericRecord> reader, Schema schema)
    {
        try {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

            List<String> outputJson = new ArrayList<>();

            while (reader.hasNext()) {
                GenericRecord record = reader.next();

                // Create output stream and encoder
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos, true);

                writer.write(record, encoder);
                encoder.flush();

                outputJson.add(baos.toString());
            }

            return outputJson;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static String goldenTablePath(String tableName)
    {
        String path = IcebergImportedTableTestBase.class.getClassLoader().getResource(tableName).getPath();
        if (path == null) {
            throw new RuntimeException("Failed to located path for resource: " + tableName);
        }
        return path;
    }

    protected static String goldenTablePathWithPrefix(String prefix, String tableName)
    {
        return goldenTablePath(prefix + FileSystems.getDefault().getSeparator() + tableName);
    }
}
