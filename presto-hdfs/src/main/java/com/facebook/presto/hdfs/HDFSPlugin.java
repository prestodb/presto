package com.facebook.presto.hdfs;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * presto-hdfs
 *
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSPlugin
implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new HDFSConnectorFactory(getClassLoader()));
    }

    private static ClassLoader getClassLoader() {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), HDFSPlugin.class.getClassLoader());
    }
}
