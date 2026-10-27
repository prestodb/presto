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
package com.facebook.presto.nativetests.iceberg;

import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * V3-only table semantics read on native workers: row lineage ({@code _row_id},
 * {@code _last_updated_sequence_number}), column default values, and equality deletes.
 */
public abstract class AbstractTestNativeIcebergV3SpecFeatures
        extends AbstractNativeIcebergV3Test
{
    protected AbstractTestNativeIcebergV3SpecFeatures(String storageFormat)
    {
        super(storageFormat);
    }

    @Test
    public void testRowLineageOnInsert()
    {
        String table = "spec_lineage_insert";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2), (3)", 3);
        assertUpdate("INSERT INTO " + table + " VALUES (4), (5)", 2);

        assertQuery("SELECT count(DISTINCT \"_row_id\"), count(\"_row_id\"), count(\"_last_updated_sequence_number\") FROM " + table, "VALUES (5, 5, 5)");
        assertQuery("SELECT count(DISTINCT \"_last_updated_sequence_number\") FROM " + table + " WHERE id <= 3", "VALUES 1");
        assertTrue(scalarLong("SELECT max(\"_last_updated_sequence_number\") FROM " + table + " WHERE id <= 3")
                        < scalarLong("SELECT min(\"_last_updated_sequence_number\") FROM " + table + " WHERE id > 3"),
                "later commits must have a larger _last_updated_sequence_number");
        // Native and Java workers must derive the same lineage from the same files.
        assertQuery("SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + table);
        dropTable(table);
    }

    @Test
    public void testRowLineageAfterUpgradeFromV2()
    {
        String table = "spec_lineage_upgrade";
        dropTable(table);
        assertUpdate(format("CREATE TABLE %s (id integer) WITH (\"format-version\" = '2', format = '%s')", table, storageFormat()));
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);
        assertQuery("SELECT count(\"_row_id\") FROM " + table, "VALUES 0");

        loadTable(table).updateProperties().set("format-version", "3").commit();
        assertUpdate("INSERT INTO " + table + " VALUES (3)", 1);
        assertQuery("SELECT count(\"_row_id\") FROM " + table + " WHERE id = 3", "VALUES 1");
        assertQuery("SELECT count(DISTINCT \"_row_id\") = count(\"_row_id\") FROM " + table, "VALUES true");
        dropTable(table);
    }

    @Test
    public void testColumnDefaultValues()
    {
        String table = "spec_defaults";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1)", 1);
        assertUpdate("ALTER TABLE " + table + " ADD COLUMN c integer DEFAULT 42");
        // initial-default fills rows written before the column existed; write-default fills omitted columns.
        assertUpdate("INSERT INTO " + table + " (id) VALUES (2)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (3, NULL)", 1);
        assertQuery("SELECT id, c FROM " + table, "VALUES (1, 42), (2, 42), (3, NULL)");
        dropTable(table);
    }

    @Test
    public void testEqualityDeletes()
            throws IOException
    {
        // V3 keeps equality deletes. Write one through the Iceberg API and read it on native workers.
        String table = "spec_equality_deletes";
        createV3Table(table, "id integer, category varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'keep'), (2, 'drop'), (3, 'keep'), (4, 'drop')", 4);

        Table icebergTable = loadTable(table);
        Schema deleteSchema = icebergTable.schema().select("category");
        OutputFile output = icebergTable.io().newOutputFile(icebergTable.location() + "/data/eq-delete-" + UUID.randomUUID() + ".parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(output)
                .forTable(icebergTable)
                .rowSchema(deleteSchema)
                .createWriterFunc(GenericParquetWriter::create)
                .equalityFieldIds(deleteSchema.findField("category").fieldId())
                .overwrite()
                .buildEqualityWriter();
        try (EqualityDeleteWriter<Record> closing = writer) {
            closing.write(GenericRecord.create(deleteSchema).copy("category", "drop"));
        }
        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        assertEquals(fileScanTasks(table).stream().map(FileScanTask::deletes).flatMap(java.util.List::stream)
                .filter(delete -> delete.content() == FileContent.EQUALITY_DELETES).count(), 1L);

        assertQuery("SELECT id FROM " + table, "VALUES 1, 3");
        assertQuery("SELECT category, count(*) FROM " + table + " GROUP BY category", "VALUES ('keep', 2)");
        dropTable(table);
    }
}
