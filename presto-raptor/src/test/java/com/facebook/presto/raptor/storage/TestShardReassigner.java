package com.facebook.presto.raptor.storage;

import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.ShardManagerDao;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.Files.createTempDir;
import static org.testng.Assert.assertTrue;

public class TestShardReassigner
{
    private ShardReassigner reassigner;
    private NodeManager nodeManager;
    private Handle handle;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        File temporary = createTempDir();
        File directory = new File(temporary, "data");
        File backupDirectory = new File(temporary, "backup");
        StorageService storageService = new FileStorageService(directory, Optional.of(backupDirectory));
        storageService.start();

        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        handle = dbi.open();
        ShardManager shardManager = new DatabaseShardManager(dbi);
        nodeManager = new InMemoryNodeManager();
        reassigner = new ShardReassigner(dbi, storageService, nodeManager, shardManager, new Duration(5, TimeUnit.MINUTES));
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        handle.close();
    }

    @Test
    public void testReassignment()
            throws Exception
    {
        UUID uuid = UUID.randomUUID();
        ShardManagerDao dao = handle.attach(ShardManagerDao.class);
        dao.insertShard(uuid);
        Node node = reassigner.reassignShard(uuid);
        assertTrue(nodeManager.getActiveNodes().contains(node));
    }
}
