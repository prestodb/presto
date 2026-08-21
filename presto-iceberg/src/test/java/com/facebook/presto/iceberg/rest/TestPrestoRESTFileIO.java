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
import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import com.facebook.presto.iceberg.HdfsInputFile;
import com.facebook.presto.iceberg.HdfsOutputFile;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.PrestoRESTFileIO;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.IcebergDistributedTestBase.getHdfsEnvironment;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.SESSION;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
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
                hdfsEnvironment());
    }

    private static HdfsEnvironment hdfsEnvironment()
    {
        return getHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig(), new HiveS3Config());
    }

    /**
     * A file IO bound to an HdfsEnvironment, as IcebergRestCatalogFactory.configureTableFileIO leaves it after a
     * table is loaded. Metadata access then goes through the Presto file system rather than the Hadoop fallback.
     */
    private PrestoRESTFileIO hdfsBackedFileIO()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());
        fileIO.setHdfsEnvironmentAndContext(hdfsEnvironment(), new HdfsContext(SESSION));
        return fileIO;
    }

    /**
     * Build a Configuration mirroring what IcebergNativeCatalogFactory.getHadoopConfiguration()
     * produces — PrestoS3ConfigurationUpdater sets fs.s3*.impl and all presto.s3.* keys.
     */
    private Configuration baseConf()
    {
        Configuration conf = new Configuration(false);
        new PrestoS3ConfigurationUpdater(new HiveS3Config()).updateConfiguration(conf);
        return conf;
    }

    @Test
    public void testSetConfAndGetConf()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        Configuration conf = baseConf();

        fileIO.setConf(conf);

        assertEquals(fileIO.getConf(), conf);
    }

    @Test
    public void testGetConfNullBeforeSetConf()
    {
        assertNull(new PrestoRESTFileIO().getConf());
    }

    @Test
    public void testPropertiesEmptyBeforeInitialize()
    {
        assertTrue(new PrestoRESTFileIO().properties().isEmpty());
    }

    @Test
    public void testInitializeStoresProperties()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        Map<String, String> props = ImmutableMap.of(
                FILE_IO_IMPL, PrestoRESTFileIO.class.getName(),
                "warehouse", warehouseLocation.getAbsolutePath());

        fileIO.initialize(props);

        assertEquals(fileIO.properties().get(FILE_IO_IMPL), PrestoRESTFileIO.class.getName());
        assertEquals(fileIO.properties().get("warehouse"), warehouseLocation.getAbsolutePath());
    }

    @Test
    public void testPropertiesReturnedAreImmutable()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.initialize(ImmutableMap.of("key", "value"));

        // ImmutableMap.put should throw UnsupportedOperationException
        boolean threw = false;
        try {
            fileIO.properties().put("other", "value");
        }
        catch (UnsupportedOperationException e) {
            threw = true;
        }
        assertTrue(threw, "properties() should return an immutable map");
    }

    @Test
    public void testHadoopConfRegistersPrestoS3FileSystemForAllSchemes()
    {
        Configuration conf = baseConf();

        assertEquals(conf.get("fs.s3.impl"), PrestoS3FileSystem.class.getName());
        assertEquals(conf.get("fs.s3a.impl"), PrestoS3FileSystem.class.getName());
        assertEquals(conf.get("fs.s3n.impl"), PrestoS3FileSystem.class.getName());
    }

    @Test
    public void testSetConfPreservesPrestoS3Impl()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());

        // setConf must not strip the fs.s3*.impl entries set by the updater
        assertEquals(fileIO.getConf().get("fs.s3.impl"), PrestoS3FileSystem.class.getName());
        assertEquals(fileIO.getConf().get("fs.s3a.impl"), PrestoS3FileSystem.class.getName());
        assertEquals(fileIO.getConf().get("fs.s3n.impl"), PrestoS3FileSystem.class.getName());
    }

    @Test
    public void testNewInputFileReturnsHadoopInputFile()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());

        String location = new Path(warehouseLocation.getAbsolutePath(), "meta.json").toUri().toString();
        InputFile inputFile = fileIO.newInputFile(location);

        assertNotNull(inputFile);
        assertTrue(inputFile instanceof HadoopInputFile,
                "expected HadoopInputFile but got " + inputFile.getClass().getName());
        assertEquals(inputFile.location(), location);
    }

    @Test
    public void testNewInputFileWithLengthReturnsHadoopInputFile()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());

        String location = new Path(warehouseLocation.getAbsolutePath(), "meta.json").toUri().toString();
        InputFile inputFile = fileIO.newInputFile(location, 1024L);

        assertNotNull(inputFile);
        assertTrue(inputFile instanceof HadoopInputFile,
                "expected HadoopInputFile but got " + inputFile.getClass().getName());
        assertEquals(inputFile.location(), location);
    }

    @Test
    public void testNewOutputFileReturnsHadoopOutputFile()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());

        String location = new Path(warehouseLocation.getAbsolutePath(), "out.json").toUri().toString();
        OutputFile outputFile = fileIO.newOutputFile(location);

        assertNotNull(outputFile);
        assertTrue(outputFile instanceof HadoopOutputFile,
                "expected HadoopOutputFile but got " + outputFile.getClass().getName());
        assertEquals(outputFile.location(), location);
    }

    @Test
    public void testSetCredentialsStoresCredentials()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());
        StorageCredential cred = StorageCredential.create(
                "s3://my-bucket/warehouse",
                ImmutableMap.of("s3.access-key-id", "VENDED_KEY", "s3.secret-access-key", "VENDED_SECRET"));

        fileIO.setCredentials(ImmutableList.of(cred));

        assertEquals(fileIO.credentials().size(), 1);
        assertEquals(fileIO.credentials().get(0).prefix(), "s3://my-bucket/warehouse");
        assertEquals(fileIO.credentials().get(0).config().get("s3.access-key-id"), "VENDED_KEY");
    }

    @Test
    public void testSetCredentialsEmptyListClearsCredentials()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());
        fileIO.setCredentials(ImmutableList.of(
                StorageCredential.create("s3://bucket/path",
                        ImmutableMap.of("s3.access-key-id", "K", "s3.secret-access-key", "S"))));

        fileIO.setCredentials(Collections.emptyList());

        assertTrue(fileIO.credentials().isEmpty());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSetCredentialsNullThrows()
    {
        new PrestoRESTFileIO().setCredentials(null);
    }

    @Test
    public void testCredentialsReturnedListIsUnmodifiable()
    {
        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());
        fileIO.setCredentials(ImmutableList.of(
                StorageCredential.create("s3://bucket/path",
                        ImmutableMap.of("s3.access-key-id", "K", "s3.secret-access-key", "S"))));

        boolean threw = false;
        try {
            fileIO.credentials().add(StorageCredential.create("s3://other",
                    ImmutableMap.of("s3.access-key-id", "K2", "s3.secret-access-key", "S2")));
        }
        catch (UnsupportedOperationException e) {
            threw = true;
        }
        assertTrue(threw, "credentials() should return an unmodifiable list");
    }

    @Test
    public void testRestCatalogUsesPrestoRESTFileIOAsIoImpl()
    {
        IcebergNativeCatalogFactory factory = getCatalogFactory();
        Catalog catalog = factory.getCatalog(SESSION);

        assertTrue(catalog instanceof RESTCatalog,
                "expected RESTCatalog but got " + catalog.getClass().getName());
        assertEquals(((RESTCatalog) catalog).properties().get(FILE_IO_IMPL),
                PrestoRESTFileIO.class.getName());
    }

    @Test
    public void testListPrefixReturnsFilesUnderLocalDirectory()
            throws Exception
    {
        // Create a small directory tree under the warehouse so listPrefix has something to find
        File subDir = new File(warehouseLocation, "ns/tbl/metadata");
        subDir.mkdirs();
        File f1 = new File(subDir, "v1.metadata.json");
        File f2 = new File(subDir, "snap-1.avro");
        f1.createNewFile();
        f2.createNewFile();

        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());

        String prefix = subDir.toURI().toString();
        Iterable<FileInfo> result = fileIO.listPrefix(prefix);

        assertNotNull(result);
        long count = 0;
        for (FileInfo fi : result) {
            assertNotNull(fi.location());
            count++;
        }
        assertEquals(count, 2L);
    }

    @Test
    public void testDeletePrefixRemovesLocalDirectory()
            throws Exception
    {
        File subDir = new File(warehouseLocation, "ns/tbl/to_delete");
        subDir.mkdirs();
        new File(subDir, "file.avro").createNewFile();
        assertTrue(subDir.exists());

        PrestoRESTFileIO fileIO = new PrestoRESTFileIO();
        fileIO.setConf(baseConf());

        fileIO.deletePrefix(subDir.toURI().toString());

        assertTrue(!subDir.exists(), "directory should have been deleted by deletePrefix");
    }

    @Test
    public void testNewInputFileUsesHdfsEnvironmentWhenBound()
    {
        String location = new Path(warehouseLocation.getAbsolutePath(), "hdfs-in.json").toUri().toString();

        InputFile inputFile = hdfsBackedFileIO().newInputFile(location);

        assertTrue(inputFile instanceof HdfsInputFile,
                "expected HdfsInputFile but got " + inputFile.getClass().getName());
        assertEquals(inputFile.location(), location);
    }

    @Test
    public void testNewInputFileWithLengthUsesHdfsEnvironmentWhenBound()
    {
        String location = new Path(warehouseLocation.getAbsolutePath(), "hdfs-in.json").toUri().toString();

        InputFile inputFile = hdfsBackedFileIO().newInputFile(location, 1024L);

        assertTrue(inputFile instanceof HdfsInputFile,
                "expected HdfsInputFile but got " + inputFile.getClass().getName());
        assertEquals(inputFile.location(), location);
    }

    @Test
    public void testNewOutputFileUsesHdfsEnvironmentWhenBound()
    {
        String location = new Path(warehouseLocation.getAbsolutePath(), "hdfs-out.json").toUri().toString();

        OutputFile outputFile = hdfsBackedFileIO().newOutputFile(location);

        assertTrue(outputFile instanceof HdfsOutputFile,
                "expected HdfsOutputFile but got " + outputFile.getClass().getName());
        assertEquals(outputFile.location(), location);
    }

    @Test
    public void testWriteThenReadThroughHdfsEnvironment()
            throws Exception
    {
        PrestoRESTFileIO fileIO = hdfsBackedFileIO();
        String location = new Path(warehouseLocation.getAbsolutePath(), "ns/tbl/hdfs_roundtrip/v1.metadata.json").toUri().toString();
        byte[] contents = "{\"format-version\":2}".getBytes(StandardCharsets.UTF_8);

        try (OutputStream out = fileIO.newOutputFile(location).create()) {
            out.write(contents);
        }

        InputFile inputFile = fileIO.newInputFile(location);
        assertTrue(inputFile.exists());
        assertEquals(inputFile.getLength(), contents.length);
        try (InputStream in = inputFile.newStream()) {
            assertEquals(new String(toByteArray(in), StandardCharsets.UTF_8), new String(contents, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testListPrefixThroughHdfsEnvironment()
            throws Exception
    {
        PrestoRESTFileIO fileIO = hdfsBackedFileIO();
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
        PrestoRESTFileIO fileIO = hdfsBackedFileIO();
        File file = new File(warehouseLocation, "ns/tbl/hdfs_delete.avro");
        file.getParentFile().mkdirs();
        file.createNewFile();
        assertTrue(file.exists());

        fileIO.deleteFile(file.toURI().toString());

        assertTrue(!file.exists(), "file should have been deleted through the Presto file system");
    }

    @Test
    public void testLoadTableBindsHdfsEnvironmentOnTableFileIO()
    {
        IcebergNativeCatalogFactory factory = getCatalogFactory();
        Catalog catalog = factory.getCatalog(SESSION);
        Namespace namespace = Namespace.of("fileio_ns");
        TableIdentifier identifier = TableIdentifier.of(namespace, "fileio_tbl");
        ((SupportsNamespaces) catalog).createNamespace(namespace);
        catalog.createTable(identifier, new Schema(Types.NestedField.optional(1, "c1", Types.LongType.get())));

        Table table = factory.loadTable(SESSION, identifier);

        assertTrue(table.io() instanceof PrestoRESTFileIO,
                "expected PrestoRESTFileIO but got " + table.io().getClass().getName());
        // configureTableFileIO must have bound the factory's HdfsEnvironment, so metadata access goes through the
        // Presto file system rather than falling back to the Hadoop file IO
        InputFile inputFile = table.io().newInputFile(new Path(table.location(), "metadata/v1.metadata.json").toUri().toString());
        assertTrue(inputFile instanceof HdfsInputFile,
                "expected HdfsInputFile but got " + inputFile.getClass().getName());
    }
}
