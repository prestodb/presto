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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.azure.HiveAzureConfig;
import com.facebook.presto.hive.azure.HiveAzureConfigurationInitializer;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.HdfsInputFile;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.iceberg.PrestoRESTFileIO;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.types.Types;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.IcebergDistributedTestBase.getHdfsEnvironment;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.SESSION;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED_DEFAULT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestPrestoRESTFileIO
{
    private File warehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

    @BeforeClass
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        serverUri = restServer.getBaseUrl().toString();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    private IcebergNativeCatalogFactory getCatalogFactory()
    {
        return getCatalogFactory(manifestFileCache(false));
    }

    private IcebergNativeCatalogFactory getCatalogFactory(ManifestFileCache manifestFileCache)
    {
        IcebergConfig icebergConfig = new IcebergConfig()
                .setCatalogType(REST)
                .setCatalogWarehouse(warehouseLocation.getAbsolutePath());
        IcebergRestConfig restConfig = new IcebergRestConfig().setServerUri(serverUri);
        return new IcebergRestCatalogFactory(
                icebergConfig,
                restConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()),
                new HiveAzureConfigurationInitializer(new HiveAzureConfig()),
                new NodeVersion("test_version"),
                hdfsEnvironment(),
                manifestFileCache);
    }

    private static ManifestFileCache manifestFileCache(boolean enabled)
    {
        return new ManifestFileCache(CacheBuilder.newBuilder().build(), enabled, Long.MAX_VALUE, 1024);
    }

    private static HdfsEnvironment hdfsEnvironment()
    {
        return getHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig(), new HiveS3Config());
    }

    private PrestoRESTFileIO fileIO(ManifestFileCache manifestFileCache)
    {
        return new PrestoRESTFileIO(hdfsEnvironment(), new HdfsContext(SESSION), manifestFileCache, ImmutableMap.of());
    }

    private String location(String name)
    {
        return new Path(warehouseLocation.getAbsolutePath(), name).toUri().toString();
    }

    private Table createTable(Catalog catalog, String namespaceName, String tableName)
    {
        Namespace namespace = Namespace.of(namespaceName);
        if (!((SupportsNamespaces) catalog).namespaceExists(namespace)) {
            ((SupportsNamespaces) catalog).createNamespace(namespace);
        }
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        catalog.createTable(identifier, new Schema(Types.NestedField.optional(1, "c1", Types.LongType.get())));
        return catalog.loadTable(identifier);
    }

    /**
     * properties() must not throw: Iceberg and Presto both read it. Iceberg's instance-keyed
     * ContentCache stays off because io.manifest.cache-enabled defaults to false and the factory
     * strips it from the catalog properties, so absence is enough.
     */
    @Test
    public void testPropertiesAreExposedAndLeaveIcebergContentCacheDisabled()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO(
                hdfsEnvironment(),
                new HdfsContext(SESSION),
                manifestFileCache(false),
                ImmutableMap.of("warehouse", "s3://bucket/warehouse"));

        assertEquals(fileIO.properties().get("warehouse"), "s3://bucket/warehouse");
        assertNull(fileIO.properties().get(IO_MANIFEST_CACHE_ENABLED));
        assertFalse(IO_MANIFEST_CACHE_ENABLED_DEFAULT, "Iceberg's ContentCache must stay off when the property is absent");
    }

    @Test
    public void testCatalogPropertiesDoNotEnableIcebergManifestCache()
    {
        IcebergRestCatalogFactory factory = (IcebergRestCatalogFactory) getCatalogFactory();
        PrestoRestCatalog catalog = (PrestoRestCatalog) factory.getCatalog(SESSION);

        assertNull(catalog.properties().get(IO_MANIFEST_CACHE_ENABLED),
                "Presto's ManifestFileCache does the caching, so Iceberg's must not be switched on");
    }

    /**
     * Iceberg calls setCredentials on the ioBuilder path only from 1.11.0, so this stays empty
     * today. Implemented so the prefixed array is captured as soon as we upgrade.
     */
    @Test
    public void testStorageCredentialsAreCapturedWithPrefixes()
    {
        PrestoRESTFileIO fileIO = fileIO(manifestFileCache(false));
        assertTrue(fileIO.credentials().isEmpty());

        fileIO.setCredentials(ImmutableList.of(
                StorageCredential.create("s3://bucket/warehouse/ns/tbl", ImmutableMap.of(
                        "s3.access-key-id", "TABLE_KEY",
                        "s3.secret-access-key", "TABLE_SECRET",
                        "s3.session-token", "TABLE_TOKEN")),
                StorageCredential.create("s3://bucket", ImmutableMap.of(
                        "s3.access-key-id", "BUCKET_KEY",
                        "s3.secret-access-key", "BUCKET_SECRET",
                        "s3.session-token", "BUCKET_TOKEN"))));

        assertEquals(fileIO.credentials().size(), 2);
        assertEquals(fileIO.credentials().get(0).prefix(), "s3://bucket/warehouse/ns/tbl");
        assertEquals(fileIO.credentials().get(0).config().get("s3.session-token"), "TABLE_TOKEN");
        assertEquals(fileIO.credentials().get(1).prefix(), "s3://bucket");
    }

    @Test
    public void testWriteThenReadThroughHdfsEnvironment()
            throws Exception
    {
        PrestoRESTFileIO fileIO = fileIO(manifestFileCache(false));
        String location = location("ns/tbl/hdfs_roundtrip/v1.metadata.json");
        byte[] contents = "{\"format-version\":2}".getBytes(UTF_8);

        try (OutputStream out = fileIO.newOutputFile(location).create()) {
            out.write(contents);
        }

        InputFile inputFile = fileIO.newInputFile(location);
        assertTrue(inputFile.exists());
        assertEquals(inputFile.getLength(), contents.length);
        try (InputStream in = inputFile.newStream()) {
            assertEquals(new String(toByteArray(in), UTF_8), new String(contents, UTF_8));
        }
    }

    @Test
    public void testListPrefixThroughHdfsEnvironment()
            throws Exception
    {
        PrestoRESTFileIO fileIO = fileIO(manifestFileCache(false));
        File subDir = new File(warehouseLocation, "ns/tbl/hdfs_list");
        subDir.mkdirs();
        new File(subDir, "v1.metadata.json").createNewFile();
        new File(subDir, "snap-1.avro").createNewFile();

        long count = 0;
        for (FileInfo fileInfo : fileIO.listPrefix(subDir.toURI().toString())) {
            assertNotNull(fileInfo.location());
            count++;
        }

        assertEquals(count, 2L);
    }

    @Test
    public void testDeleteFileThroughHdfsEnvironment()
            throws Exception
    {
        PrestoRESTFileIO fileIO = fileIO(manifestFileCache(false));
        File file = new File(warehouseLocation, "ns/tbl/hdfs_delete.avro");
        file.getParentFile().mkdirs();
        file.createNewFile();
        assertTrue(file.exists());

        fileIO.deleteFile(file.toURI().toString());

        assertTrue(!file.exists(), "file should have been deleted through the Presto file system");
    }

    @Test
    public void testDeletePrefixThroughHdfsEnvironment()
            throws Exception
    {
        PrestoRESTFileIO fileIO = fileIO(manifestFileCache(false));
        File subDir = new File(warehouseLocation, "ns/tbl/hdfs_delete_prefix");
        subDir.mkdirs();
        new File(subDir, "file.avro").createNewFile();

        fileIO.deletePrefix(subDir.toURI().toString());

        assertTrue(!new File(subDir, "file.avro").exists(), "prefix contents should have been deleted");
    }

    @Test
    public void testLoadedTableUsesPrestoRESTFileIO()
    {
        IcebergNativeCatalogFactory factory = getCatalogFactory();
        Catalog catalog = factory.getCatalog(SESSION);

        Table table = createTable(catalog, "fileio_ns", "fileio_tbl");

        assertTrue(table.io() instanceof PrestoRESTFileIO,
                "expected PrestoRESTFileIO but got " + table.io().getClass().getName());
        InputFile inputFile = table.io().newInputFile(new Path(table.location(), "metadata/v1.metadata.json").toUri().toString());
        assertTrue(inputFile instanceof HdfsInputFile,
                "expected HdfsInputFile but got " + inputFile.getClass().getName());
    }

    /**
     * The regression this design exists for: without an ioBuilder, tableFileIO returns one
     * catalog-level FileIO for every table when the server sends no per-table config.
     */
    @Test
    public void testEachTableLoadGetsItsOwnFileIO()
    {
        IcebergNativeCatalogFactory factory = getCatalogFactory();
        Catalog catalog = factory.getCatalog(SESSION);

        Table first = createTable(catalog, "isolation_ns", "tbl_one");
        Table second = createTable(catalog, "isolation_ns", "tbl_two");
        Table firstReloaded = catalog.loadTable(TableIdentifier.of(Namespace.of("isolation_ns"), "tbl_one"));

        assertNotSame(first.io(), second.io(), "different tables must not share a FileIO instance");
        assertNotSame(first.io(), firstReloaded.io(), "reloading a table must not reuse the previous FileIO instance");
    }

    /**
     * The catalog fixes the identity used for every metadata read, so it must not be shared.
     */
    @Test
    public void testCatalogIsNotSharedBetweenIdentities()
    {
        IcebergNativeCatalogFactory factory = getCatalogFactory();

        Catalog alice = factory.getCatalog(sessionForUser("alice"));
        Catalog bob = factory.getCatalog(sessionForUser("bob"));
        Catalog aliceAgain = factory.getCatalog(sessionForUser("alice"));

        assertNotSame(alice, bob, "two identities must not share a catalog");
        assertEquals(aliceAgain, alice, "the same identity should reuse its cached catalog");
    }

    private static ConnectorSession sessionForUser(String user)
    {
        return new TestingConnectorSession(
                new ConnectorIdentity(user, Optional.empty(), Optional.empty()),
                ImmutableList.of());
    }
}
