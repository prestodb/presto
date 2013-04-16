package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnFileHandle.Builder;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MockLocalStorageManager
        implements LocalStorageManager
{

    public static MockLocalStorageManager createMockLocalStorageManager()
    {
        try {
            return new MockLocalStorageManager();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private final File storageFolder;

    private MockLocalStorageManager()
            throws IOException
    {
        this(Files.createTempDir());
    }

    public MockLocalStorageManager(File storageFolder)
            throws IOException
    {
        this.storageFolder = storageFolder;
        Files.createParentDirs(this.storageFolder);
        this.storageFolder.deleteOnExit();
    }

    @Override
    public void importShard(long shardId, List<? extends ColumnHandle> columnHandles, Operator source)
            throws IOException
    {
    }

    @Override
    public BlockIterable getBlocks(long shardId, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean shardExists(long shardId)
    {
        return false;
    }

    @Override
    public void dropShard(long shardId)
    {
    }

    @Override
    public boolean isShardActive(long shardId)
    {
        return false;
    }

    @Override
    public ColumnFileHandle createStagingFileHandles(long shardId, List<? extends ColumnHandle> columnHandles)
            throws IOException
    {
        Builder builder = ColumnFileHandle.builder(shardId);
        for (ColumnHandle handle : columnHandles) {
            File tmpfile = File.createTempFile("mock-storage", "mock", storageFolder);
            tmpfile.deleteOnExit();
            builder.addColumn(handle, tmpfile);
        }
        return builder.build();
    }

    @Override
    public void commit(ColumnFileHandle columnFileHandle)
            throws IOException
    {
        columnFileHandle.commit();
    }

}
