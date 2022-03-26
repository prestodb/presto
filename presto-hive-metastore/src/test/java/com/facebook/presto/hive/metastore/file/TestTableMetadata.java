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

package com.facebook.presto.hive.metastore.file;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestTableMetadata
{
    private static final JsonCodec<TableMetadata> JSON_CODEC = JsonCodec.jsonCodec(TableMetadata.class);
    private static final String BASE_DIR = "src/test/resources/PR-17368";
    private static final String FILE_NAME_FORMAT = "table-0.271-%s.json";
    private static final String STORAGE_FORMAT_NOT_EQUALS = "storage format not equals";

    private static final StorageFormat ORC = fromHiveStorageFormat(HiveStorageFormat.ORC);
    private static final StorageFormat PARQUET = fromHiveStorageFormat(HiveStorageFormat.PARQUET);
    private static final StorageFormat CUSTOM = StorageFormat.create("serde", "inputFormat", "outputFormat");

    @Test
    public void testAssertTableMetadataEquals()
    {
        assertTableMetadataEquals(createTableMetadata(null), createTableMetadata(null));
        assertTableMetadataEquals(createTableMetadata(ORC), createTableMetadata(ORC));
        assertTableMetadataEquals(createTableMetadata(PARQUET), createTableMetadata(PARQUET));
        assertThatThrownBy(() -> assertTableMetadataEquals(createTableMetadata(null), createTableMetadata(ORC)))
                .hasMessageContaining(STORAGE_FORMAT_NOT_EQUALS);
        assertThatThrownBy(() -> assertTableMetadataEquals(createTableMetadata(PARQUET), createTableMetadata(ORC)))
                .hasMessageContaining(STORAGE_FORMAT_NOT_EQUALS);
    }

    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(createTableMetadata(null));
        assertJsonRoundTrip(createTableMetadata(ORC));
        assertJsonRoundTrip(createTableMetadata(PARQUET));
        assertJsonRoundTrip(createTableMetadata(CUSTOM));
    }

    @Test
    public void testDecodeFromLegacyFile()
            throws IOException
    {
        assertTableMetadataEquals(load("null"), createTableMetadata(null));
        assertTableMetadataEquals(load("orc"), createTableMetadata(ORC));
        assertTableMetadataEquals(load("parquet"), createTableMetadata(PARQUET));
    }

    private static TableMetadata load(String tag)
            throws IOException
    {
        return JSON_CODEC.fromBytes(Files.readAllBytes(Paths.get(BASE_DIR, format(FILE_NAME_FORMAT, tag))));
    }

    private static void assertJsonRoundTrip(TableMetadata table)
    {
        TableMetadata decoded = JSON_CODEC.fromJson(JSON_CODEC.toJson(table));
        assertTableMetadataEquals(decoded, table);
    }

    private static void assertTableMetadataEquals(TableMetadata actual, TableMetadata expected)
    {
        assertEquals(actual.getOwner(), expected.getOwner());
        assertEquals(actual.getTableType(), expected.getTableType());
        assertEquals(actual.getDataColumns(), expected.getDataColumns());
        assertEquals(actual.getPartitionColumns(), expected.getPartitionColumns());
        assertEquals(actual.getParameters(), expected.getParameters());
        assertEquals(actual.getStorageFormat(), expected.getStorageFormat(), STORAGE_FORMAT_NOT_EQUALS);
        assertEquals(actual.getBucketProperty(), expected.getBucketProperty());
        assertEquals(actual.getStorageParameters(), expected.getStorageParameters());
        assertEquals(actual.getSerdeParameters(), expected.getSerdeParameters());
        assertEquals(actual.getExternalLocation(), expected.getExternalLocation());
        assertEquals(actual.getViewOriginalText(), expected.getViewOriginalText());
        assertEquals(actual.getViewExpandedText(), expected.getViewExpandedText());
        assertEquals(actual.getColumnStatistics(), expected.getColumnStatistics());
        assertEquals(actual.getTableStorageFormat(), expected.getTableStorageFormat(), STORAGE_FORMAT_NOT_EQUALS);
    }

    private static TableMetadata createTableMetadata(StorageFormat format)
    {
        return new TableMetadata(
                "owner0",
                EXTERNAL_TABLE,
                ImmutableList.of(column("col1"), column("col2")),
                ImmutableList.of(column("part1")),
                ImmutableMap.of("param1", "value1", "param2", "value2"),
                format,
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.of("/tmp/location"),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private static Column column(String name)
    {
        return new Column(name, HIVE_STRING, Optional.of(name), Optional.empty());
    }
}
