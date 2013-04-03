package com.facebook.presto.spi;

import java.net.InetAddress;
import java.util.List;

public interface PartitionChunk
{
    long getLength();

    List<InetAddress> getHosts();

    Object getInfo();
}
