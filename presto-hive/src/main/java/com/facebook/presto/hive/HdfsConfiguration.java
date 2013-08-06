package com.facebook.presto.hive;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.SocksSocketFactory;

import javax.inject.Inject;
import javax.net.SocketFactory;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HdfsConfiguration
{
    private final HostAndPort socksProxy;
    private final Duration dfsTimeout;
    private final Duration dfsConnectTimeout;
    private final int dfsConnectMaxRetries;
    private final String domainSocketPath;

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            return createConfiguration();
        }
    };

    @Inject
    public HdfsConfiguration(HiveClientConfig hiveClientConfig)
    {
        checkNotNull(hiveClientConfig, "hiveClientConfig is null");
        checkArgument(hiveClientConfig.getDfsTimeout().toMillis() >= 1, "dfsTimeout must be at least 1 ms");

        this.socksProxy = hiveClientConfig.getMetastoreSocksProxy();
        this.dfsTimeout = hiveClientConfig.getDfsTimeout();
        this.dfsConnectTimeout = hiveClientConfig.getDfsConnectTimeout();
        this.dfsConnectMaxRetries = hiveClientConfig.getDfsConnectMaxRetries();
        this.domainSocketPath = hiveClientConfig.getDomainSocketPath();
    }

    @SuppressWarnings("UnusedParameters")
    public Configuration getConfiguration(String host)
    {
        // subclasses can provide per-host configuration
        return hadoopConfiguration.get();
    }

    protected Configuration createConfiguration()
    {
        Configuration config = new Configuration();

        // this is to prevent dfs client from doing reverse DNS lookups to determine whether nodes are rack local
        config.setClass("topology.node.switch.mapping.impl", NoOpDNSToSwitchMapping.class, DNSToSwitchMapping.class);

        if (socksProxy != null) {
            config.setClass("hadoop.rpc.socket.factory.class.default", SocksSocketFactory.class, SocketFactory.class);
            config.set("hadoop.socks.server", socksProxy.toString());
        }

        config.setBoolean("dfs.client.read.shortcircuit", true);
        if (domainSocketPath != null) {
            config.setStrings("dfs.domain.socket.path", domainSocketPath);
        }

        config.setBoolean("dfs.read.shortcircuit", true);
        config.setBoolean("dfs.read.shortcircuit.fallbackwhenfail", true);
        config.setInt("dfs.socket.timeout", Ints.checkedCast(dfsTimeout.toMillis()));
        config.setInt("ipc.ping.interval", Ints.checkedCast(dfsTimeout.toMillis()));
        config.setInt("ipc.client.connect.timeout", Ints.checkedCast(dfsConnectTimeout.toMillis()));
        config.setInt("ipc.client.connect.max.retries", dfsConnectMaxRetries);

        // Enable JMX export of stats for DFSClient
        try {
            ContextFactory factory = ContextFactory.getFactory();
            factory.setAttribute("hdfsclient.class", "org.apache.hadoop.metrics.jmx.JMXContext");
            factory.setAttribute("hdfsclient.period", "5"); // Counter value publish interval (seconds)
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return config;
    }

    public static class NoOpDNSToSwitchMapping
            implements DNSToSwitchMapping
    {
        @Override
        public List<String> resolve(List<String> names)
        {
            // dfs client expects an empty list as an indication that the host->switch mapping for the given names are not known
            return ImmutableList.of();
        }

        @Override
        public void reloadCachedMappings()
        {
            // no-op
        }
    }
}
