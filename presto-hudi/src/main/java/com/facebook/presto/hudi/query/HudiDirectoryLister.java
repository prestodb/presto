package com.facebook.presto.hudi.query;

import com.facebook.presto.hudi.HudiPartition;
import org.apache.hudi.common.model.FileSlice;

import java.io.Closeable;
import java.util.stream.Stream;

public interface HudiDirectoryLister
        extends Closeable
{
    Stream<FileSlice> listStatus(HudiPartition hudiPartition, boolean useIndex);
}
