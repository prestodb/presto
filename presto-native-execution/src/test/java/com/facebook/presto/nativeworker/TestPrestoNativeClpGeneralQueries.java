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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.mockdb.ClpMockMetadataDatabase;
import com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows;
import com.facebook.presto.plugin.clp.mockdb.table.ColumnMetadataTableRows;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.file.Paths;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static com.facebook.presto.plugin.clp.ClpQueryRunner.DEFAULT_NUM_OF_WORKERS;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.ClpString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.DateString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.NullValue;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.UnstructuredArray;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPrestoNativeClpGeneralQueries
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestPrestoNativeClpGeneralQueries.class);
    private static final String DEFAULT_TABLE_NAME = "test_e2e";
    private ClpMockMetadataDatabase mockMetadataDatabase;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        URL resource = requireNonNull(
                getClass().getClassLoader().getResource("clp-archives"),
                "Test resource 'clp-archives' not found on classpath");
        String archiveStorageDirectory = format("%s/", Paths.get(resource.toURI()));
        mockMetadataDatabase = ClpMockMetadataDatabase.builder().setArchiveStorageDirectory(archiveStorageDirectory).build();
        return PrestoNativeQueryRunnerUtils.createNativeClpQueryRunner(
                mockMetadataDatabase.getUrl(),
                mockMetadataDatabase.getUsername(),
                mockMetadataDatabase.getPassword(),
                mockMetadataDatabase.getTablePrefix());
    }

    @Override
    protected void createTables()
    {
        mockMetadataDatabase.addTableToDatasetsTableIfNotExist(ImmutableList.of(DEFAULT_TABLE_NAME));
        mockMetadataDatabase.addColumnMetadata(ImmutableMap.of(DEFAULT_TABLE_NAME, new ColumnMetadataTableRows(
                ImmutableList.of(
                        "id",
                        "msg",
                        "msg",
                        "attr.command.q._id.uid.dollar_sign_binary.sub_type",
                        "attr.existing",
                        "tags",
                        "attr.obj.md.indexes",
                        "attr.build_u_u_i_d",
                        "t.dollar_sign_date"),
                ImmutableList.of(
                        Integer,
                        ClpString,
                        VarString,
                        VarString,
                        Boolean,
                        UnstructuredArray,
                        UnstructuredArray,
                        NullValue,
                        DateString))));
        mockMetadataDatabase.addSplits(ImmutableMap.of(DEFAULT_TABLE_NAME, new ArchivesTableRows(
                ImmutableList.of("mongodb-processed-single-file-archive"),
                ImmutableList.of(1679441694576L),
                ImmutableList.of(1679442346492L))));
    }

    @Test
    public void test()
    {
        QueryRunner queryRunner = getQueryRunner();
        assertEquals(queryRunner.getNodeCount(), getNativeQueryRunnerParameters().workerCount.orElse(DEFAULT_NUM_OF_WORKERS) + 1);
        assertTrue(queryRunner.tableExists(getSession(), DEFAULT_TABLE_NAME));

        // H2QueryRunner currently can't change the timestamp format, and the default timestamp
        // format of Presto is different, so for now we have to manually format the timestamp
        // field.
        assertQuery(
                format("SELECT" +
                        " msg," +
                        " format_datetime(t.dollar_sign_date, 'yyyy-MM-dd HH:mm:ss.SSS')," +
                        " id," +
                        " attr," +
                        " tags" +
                        " FROM %s" +
                        " ORDER BY t.dollar_sign_date" +
                        " LIMIT 1", DEFAULT_TABLE_NAME),
                "SELECT" +
                        "  'Initialized wire specification'," +
                        "  TIMESTAMP '2023-03-22 12:34:54.576'," +
                        "  4915701," +
                        "  ARRAY[" +
                        "    NULL," +
                        "    ARRAY[ARRAY[ARRAY[ARRAY[ARRAY[NULL]]]]]," +
                        "    NULL," +
                        "    ARRAY[ARRAY[NULL]]" +
                        "  ]," +
                        "  NULL");
    }

    @AfterTest
    public void teardown()
    {
        if (null != mockMetadataDatabase) {
            mockMetadataDatabase.teardown();
        }
    }
}
