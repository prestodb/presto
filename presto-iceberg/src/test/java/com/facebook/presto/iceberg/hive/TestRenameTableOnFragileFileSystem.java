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
package com.facebook.presto.iceberg.hive;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cost.ConnectorFilterStatsCalculatorService;
import com.facebook.presto.cost.FilterStatsCalculator;
import com.facebook.presto.cost.ScalarStatsCalculator;
import com.facebook.presto.cost.StatsNormalizer;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.DatabaseMetadata;
import com.facebook.presto.hive.metastore.file.FileHiveMetastoreConfig;
import com.facebook.presto.hive.metastore.file.TableMetadata;
import com.facebook.presto.iceberg.CommitTaskData;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergHiveMetadata;
import com.facebook.presto.iceberg.IcebergHiveMetadataFactory;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergSessionProperties;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableName;
import com.facebook.presto.iceberg.IcebergTableProperties;
import com.facebook.presto.iceberg.IcebergTableType;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.ExpressionOptimizerProvider;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.sql.InMemoryExpressionOptimizerProvider;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HadoopExtendedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_NAME;
import static com.facebook.presto.hive.metastore.MetastoreUtil.ICEBERG_TABLE_TYPE_VALUE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.io.Files.createTempDir;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;

@Test(singleThreaded = true)
public class TestRenameTableOnFragileFileSystem
{
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = METADATA.getFunctionAndTypeManager();
    private static final StandardFunctionResolution FUNCTION_RESOLUTION = new FunctionResolution(METADATA.getFunctionAndTypeManager().getFunctionAndTypeResolver());

    private static final RowExpressionService ROW_EXPRESSION_SERVICE = new RowExpressionService()
    {
        @Override
        public DomainTranslator getDomainTranslator()
        {
            return new RowExpressionDomainTranslator(METADATA);
        }

        @Override
        public ExpressionOptimizer getExpressionOptimizer(ConnectorSession session)
        {
            return new RowExpressionOptimizer(METADATA);
        }

        @Override
        public PredicateCompiler getPredicateCompiler()
        {
            return new RowExpressionPredicateCompiler(METADATA);
        }

        @Override
        public DeterminismEvaluator getDeterminismEvaluator()
        {
            return new RowExpressionDeterminismEvaluator(METADATA);
        }

        @Override
        public String formatRowExpression(ConnectorSession session, RowExpression expression)
        {
            return new RowExpressionFormatter(METADATA.getFunctionAndTypeManager()).formatRowExpression(session, expression);
        }
    };

    private static final ExpressionOptimizerProvider EXPRESSION_OPTIMIZER_PROVIDER = new InMemoryExpressionOptimizerProvider(METADATA);
    private static final FilterStatsCalculatorService FILTER_STATS_CALCULATOR_SERVICE = new ConnectorFilterStatsCalculatorService(
            new FilterStatsCalculator(METADATA, new ScalarStatsCalculator(METADATA, EXPRESSION_OPTIMIZER_PROVIDER), new StatsNormalizer()));
    private static final JsonCodec<DatabaseMetadata> DATABASE_CODEC = jsonCodec(DatabaseMetadata.class);
    private static final JsonCodec<TableMetadata> TABLE_CODEC = jsonCodec(TableMetadata.class);

    private static final DatabaseMetadata databaseMetadata = new DatabaseMetadata(
            "owner0",
            USER,
            Optional.empty(),
            ImmutableMap.of());

    private static final TableMetadata tableMetadata = new TableMetadata(
            "owner0",
            MANAGED_TABLE,
            ImmutableList.of(column("col1"), column("col2")),
            ImmutableList.of(column("part1")),
            ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE),
            fromHiveStorageFormat(HiveStorageFormat.PARQUET),
            Optional.empty(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            ImmutableMap.of());

    private static final ConnectorSession connectorSession = new TestingConnectorSession(
            new IcebergSessionProperties(
                    new IcebergConfig(),
                    new ParquetFileWriterConfig(),
                    new OrcFileWriterConfig(),
                    new CacheConfig(),
                    Optional.empty()).getSessionProperties());

    private static final String catalogDirectory = createTempDir().toURI().toString();
    private static final String originSchemaName = "origin_schema_name";
    private static final String newSchemaName = "new_schema_name";
    private static final String originTableName = "origin_table_name";
    private static final String newTableName = "new_table_name";

    private static final String originSchemaDirPath = String.format("%s/%s", catalogDirectory, originSchemaName);
    private static final String originSchemaMetadataPath = String.format("%s/%s/%s", catalogDirectory, originSchemaName, ".prestoSchema");
    private static final String newSchemaDirPath = String.format("%s/%s", catalogDirectory, newSchemaName);
    private static final String newSchemaMetadataPath = String.format("%s/%s/%s", catalogDirectory, newSchemaName, ".prestoSchema");
    private static final String originTableMetadataPath = String.format("%s/%s/%s/%s", catalogDirectory, originSchemaName, originTableName, ".prestoSchema");
    private static final String newTableMetadataPath = String.format("%s/%s/%s/%s", catalogDirectory, newSchemaName, newTableName, ".prestoSchema");
    private static final String originTablePermissionDirPath = String.format("%s/%s/%s/%s", catalogDirectory, originSchemaName, originTableName, ".prestoPermissions");
    private static final String originTablePermissionFilePath = String.format("%s/%s/%s/%s/%s", catalogDirectory, originSchemaName, originTableName, ".prestoPermissions", "testFile");
    private static final String newTablePermissionDirPath = String.format("%s/%s/%s/%s", catalogDirectory, newSchemaName, newTableName, ".prestoPermissions");
    private static final String newTablePermissionFilePath = String.format("%s/%s/%s/%s/%s", catalogDirectory, newSchemaName, newTableName, ".prestoPermissions", "testFile");

    IcebergTableHandle icebergTableHandle = new IcebergTableHandle(originSchemaName,
            new IcebergTableName(originTableName, IcebergTableType.DATA, Optional.empty(), Optional.empty()),
            false,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            ImmutableList.of(),
            ImmutableList.of(),
            Optional.empty());

    @Test
    public void testRenameTableSucceed()
            throws Exception
    {
        testRenameTableWithFailSignalAndValidation(FailSignal.NONE,
                icebergHiveMetadata -> {
                    try {
                        icebergHiveMetadata.renameTable(connectorSession, icebergTableHandle, new SchemaTableName(newSchemaName, newTableName));
                    }
                    catch (Exception e) {
                        fail("Rename table should not fail.", e);
                    }
                },
                (icebergHiveMetadata, fileSystem) -> {
                    List<SchemaTableName> schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(newSchemaName));
                    assertTrue(schemaTableNames.contains(new SchemaTableName(newSchemaName, newTableName)));
                    schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(newSchemaName));
                    assertFalse(schemaTableNames.contains(new SchemaTableName(originSchemaName, originTableName)));

                    assertTrue(fileSystem.exists(new Path(newTableMetadataPath)));
                    assertTrue(fileSystem.exists(new Path(newTablePermissionDirPath)));
                    assertTrue(fileSystem.exists(new Path(newTablePermissionFilePath)));
                    assertFalse(fileSystem.exists(new Path(originTableMetadataPath)));
                    assertFalse(fileSystem.exists(new Path(originTablePermissionDirPath)));
                    assertFalse(fileSystem.exists(new Path(originTablePermissionFilePath)));
                });
    }

    @Test
    public void testRenameTableSucceedWithDeleteRedundantPermissionFileFails()
            throws Exception
    {
        testRenameTableWithFailSignalAndValidation(FailSignal.DELETE,
                icebergHiveMetadata -> {
                    try {
                        icebergHiveMetadata.renameTable(connectorSession, icebergTableHandle, new SchemaTableName(newSchemaName, newTableName));
                    }
                    catch (Exception e) {
                        fail("Rename table should not fail", e);
                    }
                },
                (icebergHiveMetadata, fileSystem) -> {
                    List<SchemaTableName> schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(originSchemaName));
                    assertFalse(schemaTableNames.contains(new SchemaTableName(originSchemaName, originTableName)));
                    schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(newSchemaName));
                    assertTrue(schemaTableNames.contains(new SchemaTableName(newSchemaName, newTableName)));

                    assertTrue(fileSystem.exists(new Path(newTableMetadataPath)));
                    assertTrue(fileSystem.exists(new Path(newTablePermissionDirPath)));
                    assertTrue(fileSystem.exists(new Path(newTablePermissionFilePath)));
                    assertFalse(fileSystem.exists(new Path(originTableMetadataPath)));
                    assertTrue(fileSystem.exists(new Path(originTablePermissionDirPath)));
                    assertTrue(fileSystem.exists(new Path(originTablePermissionFilePath)));
                });
    }

    @Test
    public void testRenameTableFailCausedByCopyPermissionFile()
            throws Exception
    {
        testRenameTableWithFailSignalAndValidation(FailSignal.MKDIRS,
                icebergHiveMetadata -> {
                    try {
                        icebergHiveMetadata.renameTable(connectorSession, icebergTableHandle, new SchemaTableName(newSchemaName, newTableName));
                        fail("Rename table should fail here");
                    }
                    catch (Exception e) {
                        assertTrue(e.getMessage().startsWith("Could not rename table. Failed to copy directory: "));
                    }
                },
                (icebergHiveMetadata, fileSystem) -> {
                    // The same as before
                    List<SchemaTableName> schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(originSchemaName));
                    assertTrue(schemaTableNames.contains(new SchemaTableName(originSchemaName, originTableName)));
                    schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(newSchemaName));
                    assertFalse(schemaTableNames.contains(new SchemaTableName(newSchemaName, newTableName)));

                    assertFalse(fileSystem.exists(new Path(newTableMetadataPath)));
                    assertFalse(fileSystem.exists(new Path(newTablePermissionFilePath)));
                    assertTrue(fileSystem.exists(new Path(originTableMetadataPath)));
                    assertTrue(fileSystem.exists(new Path(originTablePermissionDirPath)));
                    assertTrue(fileSystem.exists(new Path(originTablePermissionFilePath)));
                });
    }

    @Test
    public void testRenameTableFailCausedByRenameTableSchemaFile()
            throws Exception
    {
        testRenameTableWithFailSignalAndValidation(FailSignal.RENAME,
                icebergHiveMetadata -> {
                    try {
                        icebergHiveMetadata.renameTable(connectorSession, icebergTableHandle, new SchemaTableName(newSchemaName, newTableName));
                        fail("Rename table should fail here.");
                    }
                    catch (Exception e) {
                        assertTrue(e.getMessage().startsWith("Could not rename table. Failed to rename file "));
                    }
                },
                (icebergHiveMetadata, fileSystem) -> {
                    // The same as before
                    List<SchemaTableName> schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(originSchemaName));
                    assertTrue(schemaTableNames.contains(new SchemaTableName(originSchemaName, originTableName)));
                    schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(newSchemaName));
                    assertFalse(schemaTableNames.contains(new SchemaTableName(newSchemaName, newTableName)));

                    assertFalse(fileSystem.exists(new Path(newTableMetadataPath)));
                    assertFalse(fileSystem.exists(new Path(newTablePermissionFilePath)));
                    assertTrue(fileSystem.exists(new Path(originTableMetadataPath)));
                    assertTrue(fileSystem.exists(new Path(originTablePermissionDirPath)));
                    assertTrue(fileSystem.exists(new Path(originTablePermissionFilePath)));
                });
    }

    private void createInitFiles(ExtendedFileSystem fileSystem)
            throws IOException
    {
        createFile(fileSystem, new Path(originSchemaMetadataPath), DATABASE_CODEC.toBytes(databaseMetadata));
        createFile(fileSystem, new Path(newSchemaMetadataPath), DATABASE_CODEC.toBytes(databaseMetadata));
        createFile(fileSystem, new Path(originTableMetadataPath), TABLE_CODEC.toBytes(tableMetadata));
        createFile(fileSystem, new Path(originTablePermissionFilePath), new byte[128]);
    }

    private void checkInitFileSate(ExtendedFileSystem fileSystem)
            throws IOException
    {
        assertFalse(fileSystem.exists(new Path(newTableMetadataPath)));
        assertFalse(fileSystem.exists(new Path(newTablePermissionDirPath)));
        assertFalse(fileSystem.exists(new Path(newTablePermissionFilePath)));
        assertTrue(fileSystem.exists(new Path(originTableMetadataPath)));
        assertTrue(fileSystem.exists(new Path(originTablePermissionDirPath)));
        assertTrue(fileSystem.exists(new Path(originTablePermissionFilePath)));
    }

    private void testRenameTableWithFailSignalAndValidation(FailSignal failSignal, RenameLogic renameLogic, ValidationLogic validationLogic)
            throws IOException
    {
        FileHiveMetastoreConfig config = createFileHiveMetastoreConfig();
        TestingHdfsEnvironment hdfsEnvironment = getTestingHdfsEnvironment();
        IcebergFileHiveMetastore metastore = new IcebergFileHiveMetastore(hdfsEnvironment, config);
        IcebergHiveMetadata icebergHiveMetadata = (IcebergHiveMetadata) getIcebergHiveMetadata(metastore);
        ExtendedFileSystem fileSystem = hdfsEnvironment.getFileSystem(connectorSession.getUser(), new Path(originSchemaMetadataPath), new Configuration());
        try {
            createInitFiles(fileSystem);
            List<SchemaTableName> schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(originSchemaName));
            assertTrue(schemaTableNames.contains(new SchemaTableName(originSchemaName, originTableName)));
            schemaTableNames = icebergHiveMetadata.listTables(connectorSession, Optional.of(newSchemaName));
            assertFalse(schemaTableNames.contains(new SchemaTableName(newSchemaName, newTableName)));
            checkInitFileSate(fileSystem);

            if (failSignal != null && !failSignal.equals(FailSignal.NONE)) {
                hdfsEnvironment.setFailSignal(failSignal);
            }

            renameLogic.rename(icebergHiveMetadata);
            validationLogic.validate(icebergHiveMetadata, fileSystem);
        }
        finally {
            if (failSignal != null && failSignal.equals(FailSignal.DELETE)) {
                hdfsEnvironment.setFailSignal(FailSignal.NONE);
            }
            fileSystem.delete(new Path(originSchemaDirPath), true);
            fileSystem.delete(new Path(newSchemaDirPath), true);
        }
    }

    private void createFile(FileSystem fileSystem, Path path, byte[] content)
            throws IOException
    {
        FSDataOutputStream outputStream = fileSystem.create(path, true, 1024);
        outputStream.write(content);
        outputStream.flush();
        outputStream.close();
    }

    private TestingHdfsEnvironment getTestingHdfsEnvironment()
    {
        return new TestingHdfsEnvironment();
    }

    private FileHiveMetastoreConfig createFileHiveMetastoreConfig()
    {
        FileHiveMetastoreConfig config = new FileHiveMetastoreConfig();
        config.setCatalogDirectory(catalogDirectory);
        return config;
    }

    private static Column column(String name)
    {
        return new Column(name, HIVE_STRING, Optional.of(name), Optional.empty());
    }

    private ConnectorMetadata getIcebergHiveMetadata(ExtendedHiveMetastore metastore)
    {
        HdfsEnvironment hdfsEnvironment = new TestingHdfsEnvironment();
        IcebergHiveMetadataFactory icebergHiveMetadataFactory = new IcebergHiveMetadataFactory(
                new IcebergCatalogName("unimportant"),
                metastore,
                hdfsEnvironment,
                FUNCTION_AND_TYPE_MANAGER,
                FUNCTION_RESOLUTION,
                ROW_EXPRESSION_SERVICE,
                jsonCodec(CommitTaskData.class),
                jsonCodec(new TypeToken<>() {}),
                jsonCodec(new TypeToken<>() {}),
                new NodeVersion("test_node_v1"),
                FILTER_STATS_CALCULATOR_SERVICE,
                new IcebergHiveTableOperationsConfig(),
                new StatisticsFileCache(CacheBuilder.newBuilder().build()),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024),
                new IcebergTableProperties(new IcebergConfig()),
                () -> false);
        return icebergHiveMetadataFactory.create();
    }

    private interface RenameLogic
    {
        void rename(IcebergHiveMetadata icebergHiveMetadata);
    }

    private interface ValidationLogic
    {
        void validate(IcebergHiveMetadata icebergHiveMetadata, ExtendedFileSystem fileSystem)
                throws IOException;
    }

    private static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        private final AtomicReference<FailSignal> failSignal = new AtomicReference<>(FailSignal.NONE);

        public TestingHdfsEnvironment()
        {
            super(
                    new HiveHdfsConfiguration(
                            new HdfsConfigurationInitializer(new HiveClientConfig(), new MetastoreClientConfig()),
                            ImmutableSet.of(),
                            new HiveClientConfig()),
                    new MetastoreClientConfig(),
                    new NoHdfsAuthentication());
        }

        @Override
        public ExtendedFileSystem getFileSystem(String user, Path path, Configuration configuration)
        {
            return new TestingDelegateFileSystem(configuration, failSignal);
        }

        public void setFailSignal(FailSignal signal)
        {
            this.failSignal.set(signal);
        }
    }

    private enum FailSignal
    {
        MKDIRS,
        RENAME,
        DELETE,
        NONE,
    }

    private static class TestingDelegateFileSystem
            extends ExtendedFileSystem
    {
        Configuration configuration;
        HadoopExtendedFileSystem delegate;
        private final AtomicReference<FailSignal> failSignal;

        public TestingDelegateFileSystem(Configuration configuration, AtomicReference<FailSignal> failSignal)
        {
            this.configuration = configuration;
            this.failSignal = failSignal;
            LocalFileSystem localFileSystem = new LocalFileSystem();
            try {
                localFileSystem.initialize(URI.create("file:///"), configuration);
            }
            catch (IOException e) {
                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Fail to initialize LocalFileSystem");
            }
            delegate = new HadoopExtendedFileSystem(localFileSystem);
        }

        @Override
        public URI getUri()
        {
            return delegate.getUri();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
                throws IOException
        {
            return delegate.open(f, bufferSize);
        }

        @Override
        public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
                throws IOException
        {
            return delegate.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
                throws IOException
        {
            return delegate.append(f, bufferSize, progress);
        }

        @Override
        public boolean rename(Path src, Path dst)
                throws IOException
        {
            if (failSignal.get() == FailSignal.RENAME) {
                return false;
            }
            return delegate.rename(src, dst);
        }

        @Override
        public boolean delete(Path f, boolean recursive)
                throws IOException
        {
            if (failSignal.get() == FailSignal.DELETE) {
                return false;
            }
            return delegate.delete(f, recursive);
        }

        @Override
        public FileStatus[] listStatus(Path f)
                throws FileNotFoundException, IOException
        {
            return delegate.listStatus(f);
        }

        @Override
        public void setWorkingDirectory(Path newDir)
        {
            delegate.setWorkingDirectory(newDir);
        }

        @Override
        public Path getWorkingDirectory()
        {
            return delegate.getWorkingDirectory();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
                throws IOException
        {
            if (this.failSignal.get() == FailSignal.MKDIRS) {
                return false;
            }
            return delegate.mkdirs(f, permission);
        }

        @Override
        public FileStatus getFileStatus(Path f)
                throws IOException
        {
            return delegate.getFileStatus(f);
        }

        @Override
        public Configuration getConf()
        {
            return this.configuration;
        }
    }
}
