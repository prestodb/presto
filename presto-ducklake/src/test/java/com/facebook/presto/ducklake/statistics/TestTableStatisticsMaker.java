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
package com.facebook.presto.ducklake.statistics;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.DuckLakeMetadata;
import com.facebook.presto.ducklake.DuckLakeMetadataFactory;
import com.facebook.presto.ducklake.DuckLakeTableHandle;
import com.facebook.presto.ducklake.TestingDuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.ducklake.catalog.jdbc.JdbcDuckLakeCatalog;
import com.facebook.presto.ducklake.split.pruning.DataFilePruner;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Catalog-level tests for {@link TableStatisticsMaker} against the {@link TestingDuckLakeCatalog}
 * fixture, following {@code TestDuckLakeMetadata}'s style.
 */
public class TestTableStatisticsMaker
{
    private static final ConnectorSession SESSION = TestingConnectorSession.SESSION;

    private TestingDuckLakeCatalog testingCatalog;
    private JdbcDuckLakeCatalog catalog;
    private DuckLakeMetadata metadata;
    private TableStatisticsMaker tableStatisticsMaker;

    @BeforeClass
    public void setUp()
    {
        testingCatalog = new TestingDuckLakeCatalog();
        catalog = testingCatalog.createCatalog();
        TypeManager typeManager = FunctionAndTypeManager.createTestFunctionAndTypeManager();
        tableStatisticsMaker = new TableStatisticsMaker(catalog);
        metadata = new DuckLakeMetadataFactory(catalog, typeManager, tableStatisticsMaker).create();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (testingCatalog != null) {
            testingCatalog.close();
        }
    }

    @Test
    public void testByMonthRowCountWithNoConstraint()
    {
        DuckLakeTableHandle handle = getTableHandle("part", "by_month");
        TableStatistics statistics = tableStatisticsMaker.makeTableStatistics(
                handle, Optional.empty(), columnHandles(handle), new Constraint<>(TupleDomain.all()));
        assertEquals(statistics.getRowCount(), Estimate.of(400));
    }

    @Test
    public void testByMonthRowCountWithConstraintAgreesWithSplitPruning()
    {
        DuckLakeTableHandle handle = getTableHandle("part", "by_month");
        DuckLakeColumnHandle tsColumn = (DuckLakeColumnHandle) metadata.getColumnHandles(SESSION, handle).get("ts");

        // January 2024 only: PartitionPruner's month/year calendar group should resolve this
        // exactly to the one month=1/year=2024 file, out of two "month=1" files (2024 and 2025).
        long januaryStart = LocalDateTime.of(2024, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        long februaryStart = LocalDateTime.of(2024, 2, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        Domain tsDomain = Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP, januaryStart, true, februaryStart, false)), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(tsColumn, tsDomain));

        // Computed independently of TableStatisticsMaker, through the same shared pruner, so this
        // test proves "statistics and splits agree" rather than asserting a hardcoded number.
        List<DuckLakeDataFile> allFiles = catalog.listDataFiles(handle.getSnapshotId(), handle.toDuckLakeTable());
        List<DuckLakePartitionField> partitionFields = catalog.listPartitionFields(handle.getSnapshotId(), handle.getTableId());
        List<DuckLakeDataFile> survivingFiles = DataFilePruner.prune(allFiles, partitionFields, predicate);
        long expectedRowCount = survivingFiles.stream().mapToLong(DuckLakeDataFile::getRecordCount).sum();
        assertTrue(expectedRowCount < 400, "Expected the constrained row count to be below 400, got " + expectedRowCount);

        TableStatistics statistics = tableStatisticsMaker.makeTableStatistics(
                handle, Optional.empty(), columnHandles(handle), new Constraint<>(predicate));
        assertEquals(statistics.getRowCount(), Estimate.of(expectedRowCount));
    }

    @Test
    public void testByMonthTsColumnHasNoRangeButIdDoes()
    {
        DuckLakeTableHandle handle = getTableHandle("part", "by_month");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, handle);
        TableStatistics statistics = tableStatisticsMaker.makeTableStatistics(
                handle, Optional.empty(), columnHandles(handle), new Constraint<>(TupleDomain.all()));

        ColumnStatistics tsStatistics = statistics.getColumnStatistics().get(columns.get("ts"));
        assertFalse(tsStatistics.getRange().isPresent(), "Expected no range for a TIMESTAMP column");

        ColumnStatistics idStatistics = statistics.getColumnStatistics().get(columns.get("id"));
        assertTrue(idStatistics.getRange().isPresent(), "Expected a range for an INTEGER column");
    }

    @Test
    public void testPrimitivesNullsFractionForNonIdColumn()
    {
        // types.primitives has 2 rows: one fully populated, one all-NULL except "id". int_col's
        // null_count sums to 1 over a total record count of 2, so nulls fraction is exactly 0.5.
        DuckLakeTableHandle handle = getTableHandle("types", "primitives");
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, handle);
        TableStatistics statistics = tableStatisticsMaker.makeTableStatistics(
                handle, Optional.empty(), columnHandles(handle), new Constraint<>(TupleDomain.all()));

        ColumnStatistics intColStatistics = statistics.getColumnStatistics().get(columns.get("int_col"));
        assertEquals(intColStatistics.getNullsFraction(), Estimate.of(0.5));

        ColumnStatistics idStatistics = statistics.getColumnStatistics().get(columns.get("id"));
        assertEquals(idStatistics.getNullsFraction(), Estimate.of(0));
    }

    private DuckLakeTableHandle getTableHandle(String schemaName, String tableName)
    {
        return (DuckLakeTableHandle) metadata.getTableHandle(SESSION, new SchemaTableName(schemaName, tableName));
    }

    private List<ColumnHandle> columnHandles(DuckLakeTableHandle handle)
    {
        return ImmutableList.copyOf(metadata.getColumnHandles(SESSION, handle).values());
    }
}
