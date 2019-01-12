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
package io.prestosql.plugin.hive.metastore.glue;

import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveBucketProperty;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.metastore.glue.converter.GlueInputConverter;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.plugin.hive.metastore.glue.TestingMetastoreObjects.getPrestoTestDatabase;
import static io.prestosql.plugin.hive.metastore.glue.TestingMetastoreObjects.getPrestoTestPartition;
import static io.prestosql.plugin.hive.metastore.glue.TestingMetastoreObjects.getPrestoTestTable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestGlueInputConverter
{
    private final Database testDb = getPrestoTestDatabase();
    private final Table testTbl = getPrestoTestTable(testDb.getDatabaseName());
    private final Partition testPartition = getPrestoTestPartition(testDb.getDatabaseName(), testTbl.getTableName(), ImmutableList.of("val1"));

    @Test
    public void testConvertDatabase()
    {
        DatabaseInput dbInput = GlueInputConverter.convertDatabase(testDb);

        assertEquals(dbInput.getName(), testDb.getDatabaseName());
        assertEquals(dbInput.getDescription(), testDb.getComment().get());
        assertEquals(dbInput.getLocationUri(), testDb.getLocation().get());
        assertEquals(dbInput.getParameters(), testDb.getParameters());
    }

    @Test
    public void testConvertTable()
    {
        TableInput tblInput = GlueInputConverter.convertTable(testTbl);

        assertEquals(tblInput.getName(), testTbl.getTableName());
        assertEquals(tblInput.getOwner(), testTbl.getOwner());
        assertEquals(tblInput.getTableType(), testTbl.getTableType());
        assertEquals(tblInput.getParameters(), testTbl.getParameters());
        assertColumnList(tblInput.getStorageDescriptor().getColumns(), testTbl.getDataColumns());
        assertColumnList(tblInput.getPartitionKeys(), testTbl.getPartitionColumns());
        assertStorage(tblInput.getStorageDescriptor(), testTbl.getStorage());
        assertEquals(tblInput.getViewExpandedText(), testTbl.getViewExpandedText().get());
        assertEquals(tblInput.getViewOriginalText(), testTbl.getViewOriginalText().get());
    }

    @Test
    public void testConvertPartition()
    {
        PartitionInput partitionInput = GlueInputConverter.convertPartition(testPartition);

        assertEquals(partitionInput.getParameters(), testPartition.getParameters());
        assertStorage(partitionInput.getStorageDescriptor(), testPartition.getStorage());
        assertEquals(partitionInput.getValues(), testPartition.getValues());
    }

    private static void assertColumnList(List<com.amazonaws.services.glue.model.Column> actual, List<Column> expected)
    {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(com.amazonaws.services.glue.model.Column actual, Column expected)
    {
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getType(), expected.getType().getHiveTypeName().toString());
        assertEquals(actual.getComment(), expected.getComment().get());
    }

    private static void assertStorage(StorageDescriptor actual, Storage expected)
    {
        assertEquals(actual.getLocation(), expected.getLocation());
        assertEquals(actual.getSerdeInfo().getSerializationLibrary(), expected.getStorageFormat().getSerDe());
        assertEquals(actual.getInputFormat(), expected.getStorageFormat().getInputFormat());
        assertEquals(actual.getOutputFormat(), expected.getStorageFormat().getOutputFormat());

        if (expected.getBucketProperty().isPresent()) {
            HiveBucketProperty bucketProperty = expected.getBucketProperty().get();
            assertEquals(actual.getBucketColumns(), bucketProperty.getBucketedBy());
            assertEquals(actual.getNumberOfBuckets().intValue(), bucketProperty.getBucketCount());
        }
    }
}
