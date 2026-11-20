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
package com.facebook.presto.delta;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateTimeEncoding;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingConnectorSession;
import io.airlift.slice.Slices;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.delta.AbstractDeltaDistributedQueryTestBase.DELTA_SCHEMA;
import static com.facebook.presto.delta.AbstractDeltaDistributedQueryTestBase.PATH_SCHEMA;
import static com.facebook.presto.delta.AbstractDeltaDistributedQueryTestBase.goldenTablePathWithPrefix;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDeltaKernelPushdown
{
    private static final String DELTA_V3 = "delta_v3";
    private static final String TEST_KERNEL_PUSHDOWN_TABLE_NAME = "test_kernel_pushdown";
    // final version for table test_kernel_pushdown
    private static final long SNAPSHOT_VERSION = 6L;
    private final QueryRunner noPushdownQueryRunner;
    private final QueryRunner pushdownQueryRunner;

    public TestDeltaKernelPushdown()
            throws Exception
    {
        this.noPushdownQueryRunner = createQueryRunner(false);
        this.pushdownQueryRunner = createQueryRunner(true);
    }

    private static QueryRunner createQueryRunner(boolean kernelPushdown)
            throws Exception
    {
        DeltaQueryRunner.Builder queryRunnerBuilder = DeltaQueryRunner.builder().setDeltaKernelPushdown(kernelPushdown);
        return queryRunnerBuilder.build().getQueryRunner();
    }
    public static class Configuration
    {
        private final boolean pushdownEnabled;
        private final DeltaConfig config;
        private final ConnectorSession session;
        private final QueryRunner queryRunner;
        private final HdfsEnvironment hdfsEnvironment;

        public Configuration(boolean pushdownEnabled, QueryRunner queryRunner)
        {
            this.pushdownEnabled = pushdownEnabled;
            DeltaConfig deltaConfig = createDeltaConfig(pushdownEnabled);
            this.config = deltaConfig;
            this.session = new TestingConnectorSession(getSessionProperties(deltaConfig, new CacheConfig()));
            this.queryRunner = queryRunner;
            this.hdfsEnvironment = createHdfsEnvironment();
        }

        private static DeltaConfig createDeltaConfig(boolean pushdownEnabled)
        {
            DeltaConfig config = new DeltaConfig();
            config.setKernelPredicatePushdownEnabled(pushdownEnabled);
            return config;
        }

        private static List<PropertyMetadata<?>> getSessionProperties(DeltaConfig config, CacheConfig cacheConfig)
        {
            DeltaSessionProperties deltaSessionProperties = new DeltaSessionProperties(config, cacheConfig);

            return new ArrayList<>(deltaSessionProperties.getSessionProperties());
        }

        private static HdfsEnvironment createHdfsEnvironment()
        {
            HiveClientConfig hiveClientConfig = new HiveClientConfig();
            MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
            HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), Set.of(), hiveClientConfig);
            return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        }

        public boolean isPushdownEnabled()
        {
            return this.pushdownEnabled;
        }

        public DeltaConfig getConfig()
        {
            return this.config;
        }

        public ConnectorSession getSession()
        {
            return this.session;
        }

        public QueryRunner getQueryRunner()
        {
            return this.queryRunner;
        }

        public HdfsEnvironment getHdfsEnvironment()
        {
            return this.hdfsEnvironment;
        }

        @Override
        public String toString()
        {
            return pushdownEnabled ? "pushdown" : "no-pushdown";
        }
    }

    @DataProvider
    public Object[][] dataProvider()
    {
        return new Object[][] {
                {new Configuration(false, noPushdownQueryRunner)},
                {new Configuration(true, pushdownQueryRunner)}};
    }

    /**
     * computes iteration over files returned by delta kernel, with and without pushdown. It executes over the table 'test_kernel_pushdown' at version '6'
     */
    private static long computeTestKernelPushdownIteratedFiles(Configuration configuration, String columnName, Domain domain, String typeName)
    {
        return computeTestKernelPushdownIteratedFiles(configuration, columnName, domain, typeName, TEST_KERNEL_PUSHDOWN_TABLE_NAME, SNAPSHOT_VERSION);
    }

    /**
     * computes iteration over files returned by delta kernel, with and without pushdown. It executes over the given table and snapshot
     */
    private static long computeTestKernelPushdownIteratedFiles(Configuration configuration, String columnName, Domain domain, String typeName, String tableName, Long snapshotId)
    {
        DeltaColumnHandle columnHandle = new DeltaColumnHandle(
                0L,
                null,
                columnName,
                TypeSignature.parseTypeSignature(typeName),
                DeltaColumnHandle.ColumnType.REGULAR, Optional.empty());
        TupleDomain.ColumnDomain<DeltaColumnHandle> columnDomain = new TupleDomain.ColumnDomain<>(columnHandle, domain);
        ArrayList<TupleDomain.ColumnDomain<DeltaColumnHandle>> columnDomains = new ArrayList<>();
        columnDomains.add(columnDomain);
        TupleDomain<DeltaColumnHandle> tupleDomain = TupleDomain.fromColumnDomains(Optional.of(columnDomains));
        DeltaColumn column = new DeltaColumn(0L, null, columnName,
                TypeSignature.parseTypeSignature(typeName), false, false, false);
        ArrayList<DeltaColumn> columns = new ArrayList<>();
        columns.add(column);
        DeltaTable deltaTable = new DeltaTable(DELTA_SCHEMA, tableName, goldenTablePathWithPrefix(DELTA_V3, tableName), Optional.of(snapshotId), columns);
        DeltaTableHandle handle = new DeltaTableHandle("delta", deltaTable);
        DeltaTableLayoutHandle dtlh = new DeltaTableLayoutHandle(handle, tupleDomain,
                Optional.of(tupleDomain.toString(configuration.getSession().getSqlFunctionProperties())));
        DeltaClient client = new DeltaClient(configuration.getHdfsEnvironment());
        int iteratedFiles = 0;
        try (CloseableIterator<Row> batch = DeltaExpressionUtils.iterateWithPartitionPruning(
                client.listFiles(configuration.getSession(), configuration.getConfig(), dtlh, FunctionAndTypeManager.createTestFunctionAndTypeManager()),
                dtlh.getPredicate(), null)) {
            while (batch.hasNext()) {
                batch.next();
                iteratedFiles++;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return iteratedFiles;
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationBooleanColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(BooleanType.BOOLEAN, true)), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "booleansample", domain, StandardTypes.BOOLEAN);
        // expect to not file skip in both cases as non having min max stats makes it un-skippable
        // nullability checks will still benefit from file skipping
        assertEquals(iteratedFiles, 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryBooleanOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT booleansample FROM \"%s\".\"%s\" WHERE booleansample = true", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertTrue((Boolean) result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationBooleanColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(BooleanType.BOOLEAN);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "booleansample", domain, StandardTypes.BOOLEAN);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryBooleanColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT intsample FROM \"%s\".\"%s\" WHERE booleansample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationBooleanColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(BooleanType.BOOLEAN);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "booleansample", domain, StandardTypes.BOOLEAN);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryBooleanColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT booleansample FROM \"%s\".\"%s\" WHERE booleansample IS NOT NULL ORDER BY booleansample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertFalse((Boolean) result.getMaterializedRows().get(0).getField(0));
        assertFalse((Boolean) result.getMaterializedRows().get(1).getField(0));
        assertFalse((Boolean) result.getMaterializedRows().get(2).getField(0));
        assertFalse((Boolean) result.getMaterializedRows().get(3).getField(0));
        assertTrue((Boolean) result.getMaterializedRows().get(4).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationSmallIntColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(SmallintType.SMALLINT, 3L)), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "smallintsample", domain, StandardTypes.SMALLINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQuerySmallIntOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT smallintsample FROM \"%s\".\"%s\" WHERE smallintsample = 3", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), (short) 3);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationSmallIntColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(SmallintType.SMALLINT);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "smallintsample", domain, StandardTypes.SMALLINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQuerySmallIntColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT smallintsample FROM \"%s\".\"%s\" WHERE smallintsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationSmallIntColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(SmallintType.SMALLINT);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "smallintsample", domain, StandardTypes.SMALLINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQuerySmallIntColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT smallintsample FROM \"%s\".\"%s\" WHERE smallintsample IS NOT NULL ORDER BY smallintsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), (short) 1);
        assertEquals(result.getMaterializedRows().get(1).getField(0), (short) 2);
        assertEquals(result.getMaterializedRows().get(2).getField(0), (short) 3);
        assertEquals(result.getMaterializedRows().get(3).getField(0), (short) 4);
        assertEquals(result.getMaterializedRows().get(4).getField(0), (short) 5);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTinyIntColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(TinyintType.TINYINT, 3L)), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "tinyintsample", domain, StandardTypes.TINYINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTinyIntOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT tinyintsample FROM \"%s\".\"%s\" WHERE tinyintsample = 3", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), (byte) 3);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTinyIntColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(TinyintType.TINYINT);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "tinyintsample", domain, StandardTypes.TINYINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTinyIntColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT tinyintsample FROM \"%s\".\"%s\" WHERE tinyintsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTinyIntColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(TinyintType.TINYINT);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "tinyintsample", domain, StandardTypes.TINYINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTinyIntColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT tinyintsample FROM \"%s\".\"%s\" WHERE tinyintsample IS NOT NULL ORDER BY tinyintsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), (byte) 1);
        assertEquals(result.getMaterializedRows().get(1).getField(0), (byte) 2);
        assertEquals(result.getMaterializedRows().get(2).getField(0), (byte) 3);
        assertEquals(result.getMaterializedRows().get(3).getField(0), (byte) 4);
        assertEquals(result.getMaterializedRows().get(4).getField(0), (byte) 5);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationIntegerColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(IntegerType.INTEGER, 3L)), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "intsample", domain, StandardTypes.INTEGER);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryIntegerOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT intsample FROM \"%s\".\"%s\" WHERE intsample = 3", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 3);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationIntegerColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(IntegerType.INTEGER);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "intsample", domain, StandardTypes.INTEGER);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryIntegerColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT intsample FROM \"%s\".\"%s\" WHERE intsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationIntegerColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(IntegerType.INTEGER);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "intsample", domain, StandardTypes.INTEGER);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryIntegerColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT intsample FROM \"%s\".\"%s\" WHERE intsample IS NOT NULL ORDER BY intsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3);
        assertEquals(result.getMaterializedRows().get(3).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(4).getField(0), 5);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationBigIntegerColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(BigintType.BIGINT, 3L)), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "longsample", domain, StandardTypes.BIGINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryBigIntegerOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT longsample FROM \"%s\".\"%s\" WHERE longsample = 3", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 3L);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationBigIntegerColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(BigintType.BIGINT);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "longsample", domain, StandardTypes.BIGINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryBigIntegerColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT longsample FROM \"%s\".\"%s\" WHERE longsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationBigIntegerColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(BigintType.BIGINT);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "longsample", domain, StandardTypes.BIGINT);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryBigIntegerColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT longsample FROM \"%s\".\"%s\" WHERE longsample IS NOT NULL ORDER BY longsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(result.getMaterializedRows().get(3).getField(0), 4L);
        assertEquals(result.getMaterializedRows().get(4).getField(0), 5L);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationRealColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(RealType.REAL, Integer.valueOf(Float.floatToRawIntBits(3.0f)).longValue())), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "floatsample", domain, StandardTypes.REAL);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryRealOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT floatsample FROM \"%s\".\"%s\" WHERE floatsample = 3", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 3f);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationRealColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(RealType.REAL);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "floatsample", domain, StandardTypes.REAL);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryRealColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT floatsample FROM \"%s\".\"%s\" WHERE floatsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationRealColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(RealType.REAL);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "floatsample", domain, StandardTypes.REAL);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryRealColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT floatsample FROM \"%s\".\"%s\" WHERE floatsample IS NOT NULL ORDER BY floatsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1f);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 2f);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3f);
        assertEquals(result.getMaterializedRows().get(3).getField(0), 4f);
        assertEquals(result.getMaterializedRows().get(4).getField(0), 5f);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDoubleColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(DoubleType.DOUBLE, 3.0)), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "doublesample", domain, StandardTypes.DOUBLE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDoubleOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT doublesample FROM \"%s\".\"%s\" WHERE doublesample = 3", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 3.0d);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDoubleColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(DoubleType.DOUBLE);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "doublesample", domain, StandardTypes.DOUBLE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDoubleColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT doublesample FROM \"%s\".\"%s\" WHERE doublesample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDoubleColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(DoubleType.DOUBLE);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "doublesample", domain, StandardTypes.DOUBLE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDoubleColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT doublesample FROM \"%s\".\"%s\" WHERE doublesample IS NOT NULL ORDER BY doublesample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1.0d);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 2.0d);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3.0d);
        assertEquals(result.getMaterializedRows().get(3).getField(0), 4.0d);
        assertEquals(result.getMaterializedRows().get(4).getField(0), 5.0d);
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationShortDecimalColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(DecimalType.createDecimalType(1, 0), 5L)), false);
        // here it is needed to pass directly decimal(1,0) as it is a parametrized type, and StandardTypes.DECIMAL does not specify scale
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "as_big_decimal", domain, "decimal(1,0)",
                "data-reader-primitives", 1L);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 2);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryShortDecimalOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT as_big_decimal FROM \"%s\".\"%s\" WHERE as_big_decimal = 5", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "data-reader-primitives")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new BigDecimal("5"));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationShortDecimalColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(DecimalType.createDecimalType(1, 0));
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "as_big_decimal", domain, "decimal(1,0)",
                "data-reader-primitives", 1L);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 2);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryShortDecimalColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT as_big_decimal FROM \"%s\".\"%s\" WHERE as_big_decimal IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "data-reader-primitives")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationShortDecimalColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(DecimalType.createDecimalType(1, 0));
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "as_big_decimal", domain, "decimal(1,0)",
                "data-reader-primitives", 1L);
        assertEquals(iteratedFiles, 2);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryShortDecimalColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT as_big_decimal FROM \"%s\".\"%s\" WHERE as_big_decimal IS NOT NULL ORDER BY as_big_decimal ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "data-reader-primitives")));
        assertEquals(result.getMaterializedRows().size(), 10);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new BigDecimal("0"));
        assertEquals(result.getMaterializedRows().get(1).getField(0), new BigDecimal("1"));
        assertEquals(result.getMaterializedRows().get(2).getField(0), new BigDecimal("2"));
        assertEquals(result.getMaterializedRows().get(3).getField(0), new BigDecimal("3"));
        assertEquals(result.getMaterializedRows().get(4).getField(0), new BigDecimal("4"));
        assertEquals(result.getMaterializedRows().get(5).getField(0), new BigDecimal("5"));
        assertEquals(result.getMaterializedRows().get(6).getField(0), new BigDecimal("6"));
        assertEquals(result.getMaterializedRows().get(7).getField(0), new BigDecimal("7"));
        assertEquals(result.getMaterializedRows().get(8).getField(0), new BigDecimal("8"));
        assertEquals(result.getMaterializedRows().get(9).getField(0), new BigDecimal("9"));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDecimalColumn(Configuration configuration)
    {
        BigInteger bigint = BigInteger.valueOf(3L);
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(DecimalType.createDecimalType(38, 18),
                Decimals.encodeScaledValue(new BigDecimal(bigint), 18))), false);
        // here it is needed to pass directly decimal(38,18) as it is a parametrized type, and StandardTypes.DECIMAL does not specify scale
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "decimalsample", domain, "decimal(38,18)");
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDecimalOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT decimalsample FROM \"%s\".\"%s\" WHERE decimalsample = 3", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new BigDecimal("3.000000000000000000"));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDecimalColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(DecimalType.createDecimalType(38, 18));
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "decimalsample", domain, StandardTypes.DECIMAL);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDecimalColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT decimalsample FROM \"%s\".\"%s\" WHERE decimalsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDecimalColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(DecimalType.createDecimalType(38, 18));
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "decimalsample", domain, StandardTypes.DECIMAL);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDecimalColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT decimalsample FROM \"%s\".\"%s\" WHERE decimalsample IS NOT NULL ORDER BY decimalsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new BigDecimal("1.000000000000000000"));
        assertEquals(result.getMaterializedRows().get(1).getField(0), new BigDecimal("2.000000000000000000"));
        assertEquals(result.getMaterializedRows().get(2).getField(0), new BigDecimal("3.000000000000000000"));
        assertEquals(result.getMaterializedRows().get(3).getField(0), new BigDecimal("4.000000000000000000"));
        assertEquals(result.getMaterializedRows().get(4).getField(0), new BigDecimal("5.000000000000000000"));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationVarcharColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("test with spaces 4"))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "stringsample", domain, StandardTypes.VARCHAR);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryVarcharOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT stringsample FROM \"%s\".\"%s\" WHERE stringsample = 'test with spaces 4'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "test with spaces 4");
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationVarcharColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(DoubleType.DOUBLE);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "stringsample", domain, StandardTypes.DOUBLE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryVarcharColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT stringsample FROM \"%s\".\"%s\" WHERE stringsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationVarcharColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(DoubleType.DOUBLE);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "stringsample", domain, StandardTypes.DOUBLE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryVarcharColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT stringsample FROM \"%s\".\"%s\" WHERE stringsample IS NOT NULL ORDER BY stringsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "test with spaces");
        assertEquals(result.getMaterializedRows().get(1).getField(0), "test with spaces");
        assertEquals(result.getMaterializedRows().get(2).getField(0), "test with spaces 4");
        assertEquals(result.getMaterializedRows().get(3).getField(0), "test with spaces 5");
        assertEquals(result.getMaterializedRows().get(4).getField(0), "test1");
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationVarbinaryColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(VarbinaryType.VARBINARY, Slices.wrappedBuffer(new byte[]{(byte) 0xFF, (byte) 0xF2}))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "binarysample", domain, StandardTypes.VARBINARY);
        // stats for binaries do not have min max values so cant file skip
        // nullability checks will still benefit from pushdown
        assertEquals(iteratedFiles, 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryVarbinaryOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT binarysample FROM \"%s\".\"%s\" WHERE binarysample = X'FFF2'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new byte[]{(byte) 0xFF, (byte) 0xF2});
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationVarbinaryColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(VarbinaryType.VARBINARY);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "binarysample", domain, StandardTypes.VARBINARY);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryVarbinaryColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT binarysample FROM \"%s\".\"%s\" WHERE binarysample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationVarbinaryColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(VarbinaryType.VARBINARY);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "binarysample", domain, StandardTypes.VARBINARY);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryVarbinaryColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT binarysample FROM \"%s\".\"%s\" WHERE binarysample IS NOT NULL ORDER BY binarysample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new byte[]{(byte) 0xFF, (byte) 0xF0});
        assertEquals(result.getMaterializedRows().get(1).getField(0), new byte[]{(byte) 0xFF, (byte) 0xF1});
        assertEquals(result.getMaterializedRows().get(2).getField(0), new byte[]{(byte) 0xFF, (byte) 0xF2});
        assertEquals(result.getMaterializedRows().get(3).getField(0), new byte[]{(byte) 0xFF, (byte) 0xF3});
        assertEquals(result.getMaterializedRows().get(4).getField(0), new byte[]{(byte) 0xFF, (byte) 0xF4});
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDateColumn(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(DateType.DATE, LocalDate.of(2026, 9, 18).toEpochDay())), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "datesampele", domain, StandardTypes.DATE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 2 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDateOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT datesampele FROM \"%s\".\"%s\" WHERE datesampele = DATE '2026-09-18'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), LocalDate.of(2026, 9, 18));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDateColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(DateType.DATE);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "datesampele", domain, StandardTypes.DATE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDateColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT datesampele FROM \"%s\".\"%s\" WHERE datesampele IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationDateColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(DateType.DATE);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "datesampele", domain, StandardTypes.DATE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDateColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT datesampele FROM \"%s\".\"%s\" WHERE datesampele IS NOT NULL ORDER BY datesampele ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), LocalDate.of(2026, 9, 17));
        assertEquals(result.getMaterializedRows().get(1).getField(0), LocalDate.of(2026, 9, 18));
        assertEquals(result.getMaterializedRows().get(2).getField(0), LocalDate.of(2026, 9, 19));
        assertEquals(result.getMaterializedRows().get(3).getField(0), LocalDate.of(2026, 9, 20));
        assertEquals(result.getMaterializedRows().get(4).getField(0), LocalDate.of(2026, 9, 21));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTimestampNTZColumn(Configuration configuration)
    {
        Instant millisUtc = Instant.parse("2026-09-18T18:18:18.00Z");
        Type type = TimestampType.TIMESTAMP;
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.equal(type, millisUtc.toEpochMilli())), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "timestampntzsample", domain, StandardTypes.TIMESTAMP);
        // due to issues in timestamp64 table, disabled pushdown for now. We only check that the old behavior is preserved even with pushdown enabled
        assertEquals(iteratedFiles, 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTimestampNTZOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT timestampntzsample FROM \"%s\".\"%s\" WHERE timestampntzsample = TIMESTAMP '2026-09-18 18:18:18'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), LocalDateTime.of(2026, 9, 18, 18, 18, 18));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTimestampNTZColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(TimestampType.TIMESTAMP);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "timestampntzsample", domain, StandardTypes.TIMESTAMP);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTimestampNTZColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT timestampntzsample FROM \"%s\".\"%s\" WHERE timestampntzsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTimestampNTZColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(TimestampType.TIMESTAMP);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "timestampntzsample", domain, StandardTypes.TIMESTAMP);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTimestampNTZColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT timestampntzsample FROM \"%s\".\"%s\" WHERE timestampntzsample IS NOT NULL ORDER BY timestampntzsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(0), LocalDateTime.of(2026, 9, 17, 17, 17, 17));
        assertEquals(result.getMaterializedRows().get(1).getField(0), LocalDateTime.of(2026, 9, 18, 18, 18, 18));
        assertEquals(result.getMaterializedRows().get(2).getField(0), LocalDateTime.of(2026, 9, 19, 19, 19, 19));
        assertEquals(result.getMaterializedRows().get(3).getField(0), LocalDateTime.of(2026, 9, 20, 20, 20, 20));
        assertEquals(result.getMaterializedRows().get(4).getField(0), LocalDateTime.of(2026, 9, 21, 21, 21, 21));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTimestampColumn(Configuration configuration)
    {
        Instant millisUtc = Instant.parse("2026-09-18T16:18:18.00Z");
        Type type = TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(type,
                DateTimeEncoding.packDateTimeWithZone(millisUtc.toEpochMilli(), "Europe/Madrid"))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "timestampsample", domain, StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        // due to issues in timestamp64 table, disabled pushdown for now. We only check that the old behavior is preserved even with pushdown enabled
        assertEquals(iteratedFiles, 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTimestampOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT timestampsample FROM \"%s\".\"%s\" WHERE timestampsample = TIMESTAMP '2026-09-18 18:18:18 Europe/Madrid'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0),
                // -2 hours from Europe/Madrid to UTC timezone
                LocalDateTime.of(2026, 9, 18, 16, 18, 18).atZone(ZoneId.of("UTC")));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTimestampColumnIsNull(Configuration configuration)
    {
        Domain domain = Domain.onlyNull(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "timestampsample", domain, StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTimestampColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT timestampsample FROM \"%s\".\"%s\" WHERE timestampsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationTimestampColumnIsNotNull(Configuration configuration)
    {
        Domain domain = Domain.notNull(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "timestampsample", domain, StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 5 : 6);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryTimestampColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT timestampsample FROM \"%s\".\"%s\" WHERE timestampsample IS NOT NULL ORDER BY timestampsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        // -2 hours from Europe/Madrid to UTC timezone
        assertEquals(result.getMaterializedRows().get(0).getField(0), LocalDateTime.of(2026, 9, 17, 15, 17, 17).atZone(ZoneId.of("UTC")));
        assertEquals(result.getMaterializedRows().get(1).getField(0), LocalDateTime.of(2026, 9, 18, 16, 18, 18).atZone(ZoneId.of("UTC")));
        assertEquals(result.getMaterializedRows().get(2).getField(0), LocalDateTime.of(2026, 9, 19, 17, 19, 19).atZone(ZoneId.of("UTC")));
        assertEquals(result.getMaterializedRows().get(3).getField(0), LocalDateTime.of(2026, 9, 20, 18, 20, 20).atZone(ZoneId.of("UTC")));
        assertEquals(result.getMaterializedRows().get(4).getField(0), LocalDateTime.of(2026, 9, 21, 19, 21, 21).atZone(ZoneId.of("UTC")));
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryArrayOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT arraysample FROM \"%s\".\"%s\" WHERE arraysample = ARRAY[3]", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        ArrayList<Integer> expected = new ArrayList<>(1);
        expected.add(3);
        assertEquals(result.getMaterializedRows().get(0).getField(0), expected);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryArrayColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT arraysample FROM \"%s\".\"%s\" WHERE arraysample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryArrayColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT arraysample FROM \"%s\".\"%s\" WHERE arraysample IS NOT NULL ORDER BY arraysample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        ArrayList<Integer> expected = new ArrayList<>(1);
        expected.add(1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), expected);
        expected.clear();
        expected.add(2);
        assertEquals(result.getMaterializedRows().get(1).getField(0), expected);
        expected.clear();
        expected.add(3);
        assertEquals(result.getMaterializedRows().get(2).getField(0), expected);
        expected.clear();
        expected.add(4);
        assertEquals(result.getMaterializedRows().get(3).getField(0), expected);
        expected.clear();
        expected.add(5);
        assertEquals(result.getMaterializedRows().get(4).getField(0), expected);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryMapOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT mapsample FROM \"%s\".\"%s\" WHERE mapsample = MAP(ARRAY[3], ARRAY[3])", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        Map<Integer, Integer> expected = new HashMap<>(1);
        expected.put(3, 3);
        assertEquals(result.getMaterializedRows().get(0).getField(0), expected);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryMapColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT mapsample FROM \"%s\".\"%s\" WHERE mapsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryMapColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT mapsample FROM \"%s\".\"%s\" WHERE mapsample IS NOT NULL ORDER BY intsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        Map<Integer, Integer> expected = new HashMap<>(1);
        expected.put(1, 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), expected);
        expected.clear();
        expected.put(2, 2);
        assertEquals(result.getMaterializedRows().get(1).getField(0), expected);
        expected.clear();
        expected.put(3, 3);
        assertEquals(result.getMaterializedRows().get(2).getField(0), expected);
        expected.clear();
        expected.put(4, 4);
        assertEquals(result.getMaterializedRows().get(3).getField(0), expected);
        expected.clear();
        expected.put(5, 5);
        assertEquals(result.getMaterializedRows().get(4).getField(0), expected);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryStructOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT structsample FROM \"%s\".\"%s\" WHERE structsample = ROW(3)", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        ArrayList<Integer> expected = new ArrayList<>(1);
        expected.add(3);
        assertEquals(result.getMaterializedRows().get(0).getField(0), expected);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryStructColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT structsample FROM \"%s\".\"%s\" WHERE structsample IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertNull(result.getMaterializedRows().get(0).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryStructColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT structsample FROM \"%s\".\"%s\" WHERE structsample IS NOT NULL ORDER BY intsample ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_kernel_pushdown")));
        assertEquals(result.getMaterializedRows().size(), 5);
        ArrayList<Integer> expected = new ArrayList<>(1);
        expected.add(1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), expected);
        expected.clear();
        expected.add(2);
        assertEquals(result.getMaterializedRows().get(1).getField(0), expected);
        expected.clear();
        expected.add(3);
        assertEquals(result.getMaterializedRows().get(2).getField(0), expected);
        expected.clear();
        expected.add(4);
        assertEquals(result.getMaterializedRows().get(3).getField(0), expected);
        expected.clear();
        expected.add(5);
        assertEquals(result.getMaterializedRows().get(4).getField(0), expected);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryVariantOutput(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT data FROM \"%s\".\"%s\" WHERE json_format(data) = json_format(CAST(CAST(ROW(false, 42, 'manager', 'eve') AS " +
                                "ROW(active BOOLEAN, age INTEGER, role VARCHAR, \"user\" VARCHAR)) AS JSON))", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_variant")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "{\"active\":false,\"age\":42,\"role\":\"manager\",\"user\":\"eve\"}");
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryVariantColumnIsNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT data FROM \"%s\".\"%s\" WHERE data IS NULL", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_variant")));
        assertEquals(result.getMaterializedRows().size(), 4);
        assertNull(result.getMaterializedRows().get(0).getField(0));
        assertNull(result.getMaterializedRows().get(1).getField(0));
        assertNull(result.getMaterializedRows().get(2).getField(0));
        assertNull(result.getMaterializedRows().get(3).getField(0));
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryDataColumnIsNotNull(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT data FROM \"%s\".\"%s\" WHERE data IS NOT NULL ORDER BY id ASC", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "test_variant")));
        assertEquals(result.getMaterializedRows().size(), 8);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "{\"active\":true,\"age\":30,\"user\":\"alice\"}");
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationColumnMappingById(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("Eva"))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "given_name", domain, StandardTypes.VARCHAR, "cm_id", 4L);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 2);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryFilterColumMappingById(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE given_name = 'Eva'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "cm_id")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "Eva");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "Martinez");
        assertEquals(result.getMaterializedRows().get(0).getField(3), "eva.martinez@example.com");
        assertEquals(result.getMaterializedRows().get(0).getField(4), LocalDate.of(2026, 1, 20));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationColumnMappingByIdNonHivePartitioning(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("Eva"))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "given_name", domain, StandardTypes.VARCHAR, "cm_id_partitioned", 4L);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 5);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryFilterColumMappingByIdAndNonHivePartitioning(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE given_name = 'Eva'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "cm_id")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "Eva");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "Martinez");
        assertEquals(result.getMaterializedRows().get(0).getField(3), "eva.martinez@example.com");
        assertEquals(result.getMaterializedRows().get(0).getField(4), LocalDate.of(2026, 1, 20));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationColumnMappingByName(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("Eva"))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "given_name", domain, StandardTypes.VARCHAR, "cm_name", 4L);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 2);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryFilterColumMappingByName(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE given_name = 'Eva'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "cm_name")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "Eva");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "Martinez");
        assertEquals(result.getMaterializedRows().get(0).getField(3), "eva.martinez@example.com");
        assertEquals(result.getMaterializedRows().get(0).getField(4), LocalDate.of(2026, 1, 20));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationColumnMappingByNameNonHivePartitioning(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("Eva"))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "given_name", domain, StandardTypes.VARCHAR, "cm_name_partitioned", 4L);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 5);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryFilterColumMappingByNameAndNonHivePartitioning(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE given_name = 'Eva'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "cm_name")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "Eva");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "Martinez");
        assertEquals(result.getMaterializedRows().get(0).getField(3), "eva.martinez@example.com");
        assertEquals(result.getMaterializedRows().get(0).getField(4), LocalDate.of(2026, 1, 20));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationColumnMappingSpecialCharacters(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("eva.martinez@example.com"))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "e-mail (address)", domain, StandardTypes.VARCHAR, "cm_sp_char", 6L);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 2);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryFilterColumMappingSpecialCharacters(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"e-mail (address)\" = 'eva.martinez@example.com'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "cm_sp_char")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "Eva");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "Martinez");
        assertEquals(result.getMaterializedRows().get(0).getField(3), "eva.martinez@example.com");
        assertEquals(result.getMaterializedRows().get(0).getField(4), "555-0105");
        assertEquals(result.getMaterializedRows().get(0).getField(5), LocalDate.of(2026, 1, 20));
    }

    @Test(dataProvider = "dataProvider")
    public void testIterationColumnMappingSpecialCharactersNonHivePartitioning(Configuration configuration)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(VarcharType.VARCHAR, Slices.utf8Slice("eva.martinez@example.com"))), false);
        long iteratedFiles = computeTestKernelPushdownIteratedFiles(configuration, "e-mail (address)", domain, StandardTypes.VARCHAR, "cm_sp_char_partitioned", 6L);
        assertEquals(iteratedFiles, configuration.isPushdownEnabled() ? 1 : 5);
    }

    @Test(dataProvider = "dataProvider")
    public void testQueryFilterColumMappingSpecialCharactersAndNonHivePartitioning(Configuration configuration)
    {
        MaterializedResult result = configuration.getQueryRunner().execute(
                String.format("SELECT * FROM \"%s\".\"%s\" WHERE \"e-mail (address)\" = 'eva.martinez@example.com'", PATH_SCHEMA,
                        goldenTablePathWithPrefix(DELTA_V3, "cm_sp_char")));
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "Eva");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "Martinez");
        assertEquals(result.getMaterializedRows().get(0).getField(3), "eva.martinez@example.com");
        assertEquals(result.getMaterializedRows().get(0).getField(4), "555-0105");
        assertEquals(result.getMaterializedRows().get(0).getField(5), LocalDate.of(2026, 1, 20));
    }
}
