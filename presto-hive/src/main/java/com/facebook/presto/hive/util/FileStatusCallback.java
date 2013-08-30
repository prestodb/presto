package com.facebook.presto.hive.util;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;

public interface FileStatusCallback
{
    /**
     * Called once for each FileStatus encountered in the path traversal
     */
    void process(FileStatus fileStatus, BlockLocation[] blockLocations);
}
