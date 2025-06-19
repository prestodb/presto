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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.math3.util.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.clp.ClpMetadata.DEFAULT_SCHEMA_NAME;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.DbHandle;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.getDbHandle;
import static com.facebook.presto.plugin.clp.ClpMetadataDbSetUp.setupMetadata;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Boolean;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.ClpString;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Float;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.Integer;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.UnstructuredArray;
import static com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType.VarString;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestClpMetadata
{
    private static final String TABLE_NAME = "test";
    private DbHandle dbHandle;
    private ClpMetadata metadata;

    @BeforeMethod
    public void setUp()
    {
        dbHandle = getDbHandle("metadata_testdb");
        metadata = setupMetadata(
                dbHandle,
                ImmutableMap.of(
                        TABLE_NAME,
                        ImmutableList.of(
                                new Pair<>("a", Integer),
                                new Pair<>("a", ClpString),
                                new Pair<>("b", Float),
                                new Pair<>("b", ClpString),
                                new Pair<>("c.d", Boolean),
                                new Pair<>("c.e", VarString),
                                new Pair<>("f.g.h", UnstructuredArray))));
    }

    @AfterMethod
    public void tearDown()
    {
        ClpMetadataDbSetUp.tearDown(dbHandle);
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of(DEFAULT_SCHEMA_NAME));
    }

    @Test
    public void testListTables()
    {
        HashSet<SchemaTableName> tables = new HashSet<>();
        tables.add(new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.empty())), tables);
    }

    @Test
    public void testGetTableMetadata()
    {
        ClpTableHandle clpTableHandle = (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(DEFAULT_SCHEMA_NAME, TABLE_NAME));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, clpTableHandle);
        ImmutableSet<ColumnMetadata> columnMetadata = ImmutableSet.<ColumnMetadata>builder()
                .add(ColumnMetadata.builder()
                        .setName("a_bigint")
                        .setType(BIGINT)
                        .setNullable(true)
                        .build())
                .add(ColumnMetadata.builder()
                        .setName("a_varchar")
                        .setType(VARCHAR)
                        .setNullable(true)
                        .build())
                .add(ColumnMetadata.builder()
                        .setName("b_double")
                        .setType(DOUBLE)
                        .setNullable(true)
                        .build())
                .add(ColumnMetadata.builder()
                        .setName("b_varchar")
                        .setType(VARCHAR)
                        .setNullable(true)
                        .build())
                .add(ColumnMetadata.builder()
                        .setName("c")
                        .setType(RowType.from(ImmutableList.of(
                                RowType.field("d", BOOLEAN),
                                RowType.field("e", VARCHAR))))
                        .setNullable(true)
                        .build())
                .add(ColumnMetadata.builder()
                        .setName("f")
                        .setType(RowType.from(ImmutableList.of(
                                RowType.field("g",
                                        RowType.from(ImmutableList.of(
                                                RowType.field("h", new ArrayType(VARCHAR))))))))
                        .setNullable(true)
                        .build())
                .build();
        assertEquals(columnMetadata, ImmutableSet.copyOf(tableMetadata.getColumns()));
    }
}
