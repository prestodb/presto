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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.clp.metadata.ClpMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpNodeType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.util.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestClpMetadata
{
    private ClpMetadata metadata;

    private static final String TABLE_NAME = "test";
    private static final String TABLE_SCHEMA = "default";

    @BeforeMethod
    public void setUp()
    {
        final String metadataDbUrl = "jdbc:h2:file:/tmp/metadata_testdb;MODE=MySQL;DATABASE_TO_UPPER=FALSE";
        final String metadataDbUser = "sa";
        final String metadataDbPassword = "";
        final String metadataDbTablePrefix = "clp_";
        final String columnMetadataTablePrefix = "column_metadata_";
        final String tableMetadataSuffix = "table_metadata";

        ClpConfig config = new ClpConfig().setPolymorphicTypeEnabled(true)
                .setMetadataDbUrl(metadataDbUrl)
                .setMetadataDbUser("sa")
                .setMetadataDbPassword("")
                .setMetadataTablePrefix(metadataDbTablePrefix);
        ClpMetadataProvider metadataProvider = new ClpMySqlMetadataProvider(config);
        metadata = new ClpMetadata(config, metadataProvider);

        final String tableMetadataTableName = metadataDbTablePrefix + tableMetadataSuffix;
        final String columnMetadataTableName = metadataDbTablePrefix + columnMetadataTablePrefix + TABLE_NAME;

        final String createTableMetadataSQL = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        " table_name VARCHAR(512) PRIMARY KEY," +
                        " table_path VARCHAR(1024) NOT NULL)", tableMetadataTableName);

        final String createColumnMetadataSQL = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        " name VARCHAR(512) NOT NULL," +
                        " type TINYINT NOT NULL," +
                        " PRIMARY KEY (name, type))", columnMetadataTableName);

        final String insertTableMetadataSQL = String.format(
                "INSERT INTO %s (table_name, table_path) VALUES (?, ?)", tableMetadataTableName);

        final String insertColumnMetadataSQL = String.format(
                "INSERT INTO %s (name, type) VALUES (?, ?)", columnMetadataTableName);

        try (Connection conn = DriverManager.getConnection(metadataDbUrl, metadataDbUser, metadataDbPassword);
                Statement stmt = conn.createStatement()) {
            stmt.execute(createTableMetadataSQL);
            stmt.execute(createColumnMetadataSQL);

            // Insert table metadata
            try (PreparedStatement pstmt = conn.prepareStatement(insertTableMetadataSQL)) {
                pstmt.setString(1, TABLE_NAME);
                pstmt.setString(2, "/tmp/archives/" + TABLE_NAME);
                pstmt.executeUpdate();
            }

            // Insert column metadata in batch
            List<Pair<String, ClpNodeType>> records = Arrays.asList(
                    new Pair<>("a", ClpNodeType.Integer),
                    new Pair<>("a", ClpNodeType.VarString),
                    new Pair<>("b", ClpNodeType.Float),
                    new Pair<>("b", ClpNodeType.ClpString),
                    new Pair<>("c.d", ClpNodeType.Boolean),
                    new Pair<>("c.e", ClpNodeType.VarString),
                    new Pair<>("f.g.h", ClpNodeType.UnstructuredArray));

            try (PreparedStatement pstmt = conn.prepareStatement(insertColumnMetadataSQL)) {
                for (Pair<String, ClpNodeType> record : records) {
                    pstmt.setString(1, record.getFirst());
                    pstmt.setByte(2, record.getSecond().getType());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @AfterMethod
    public void tearDown()
    {
        File dbFile = new File("/tmp/metadata_testdb.mv.db");
        File lockFile = new File("/tmp/metadata_testdb.trace.db"); // Optional, H2 sometimes creates this
        if (dbFile.exists()) {
            dbFile.delete();
            System.out.println("Deleted database file: " + dbFile.getAbsolutePath());
        }
        if (lockFile.exists()) {
            lockFile.delete();
        }
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableList.of(TABLE_SCHEMA));
    }

    @Test
    public void testListTables()
    {
        HashSet<SchemaTableName> tables = new HashSet<>();
        tables.add(new SchemaTableName(TABLE_SCHEMA, TABLE_NAME));
        assertEquals(new HashSet<>(metadata.listTables(SESSION, Optional.empty())), tables);
    }

    @Test
    public void testGetTableMetadata()
    {
        ClpTableHandle clpTableHandle =
                (ClpTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(TABLE_SCHEMA, TABLE_NAME));
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, clpTableHandle);
        HashSet<ColumnMetadata> columnMetadata = new HashSet<>();
        columnMetadata.add(ColumnMetadata.builder()
                .setName("a_bigint")
                .setType(BigintType.BIGINT)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("a_varchar")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b_double")
                .setType(DoubleType.DOUBLE)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("b_varchar")
                .setType(VarcharType.VARCHAR)
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("c")
                .setType(RowType.from(ImmutableList.of(
                        RowType.field("d", BooleanType.BOOLEAN),
                        RowType.field("e", VarcharType.VARCHAR))))
                .setNullable(true)
                .build());
        columnMetadata.add(ColumnMetadata.builder()
                .setName("f")
                .setType(RowType.from(ImmutableList.of(
                        RowType.field("g",
                                RowType.from(ImmutableList.of(
                                        RowType.field("h", new ArrayType(VarcharType.VARCHAR))))))))
                .setNullable(true)
                .build());
        assertEquals(columnMetadata, new HashSet<>(tableMetadata.getColumns()));
    }
}
