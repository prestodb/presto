package com.facebook.presto.server.testing;

import com.facebook.airlift.node.NodeConfig;
import com.facebook.airlift.node.NodeInfo;
import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.weakref.jmx.guice.ExportBinder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TestPrestoServerNodeModule
        implements Module
{
    private static final AtomicLong nextId = new AtomicLong((long) ThreadLocalRandom.current().nextInt(1000000));
    private final String environment;
    private final Optional<String> pool;

    public TestPrestoServerNodeModule()
    {
        this(Optional.empty());
    }

    public TestPrestoServerNodeModule(Optional<String> environment)
    {
        this((String) environment.orElse("test" + nextId.getAndIncrement()));
    }

    public TestPrestoServerNodeModule(String environment)
    {
        this(environment, Optional.empty());
    }

    public TestPrestoServerNodeModule(String environment, Optional<String> pool)
    {
        checkArgument(!Strings.isNullOrEmpty(environment), "environment is null or empty");
        this.environment = environment;
        this.pool = (Optional) requireNonNull(pool, "pool is null");
    }

    public TestPrestoServerNodeModule(String environment, String pool)
    {
        this(environment, Optional.of(requireNonNull(pool, "pool is null")));
    }

    public void configure(Binder binder)
    {
        binder.bind(NodeInfo.class).in(Scopes.SINGLETON);
        Optional<String> nodeIDPrefix = Optional.ofNullable(System.getProperty("node.id.prefix"));
        String defaultNodeID = UUID.randomUUID().toString();
        String nodeId = nodeIDPrefix.map(
                id -> new StringBuilder("node-")
                        .append(id)
                        .append("-UUID-")
                        .append(defaultNodeID).toString())
                .orElse(defaultNodeID);
        NodeConfig nodeConfig = (new NodeConfig())
                .setEnvironment(this.environment)
                .setNodeInternalAddress(InetAddresses.toAddrString(getV4Localhost()))
                .setNodeBindIp(getV4Localhost())
                .setNodeId(nodeId);
        if (this.pool.isPresent()) {
            nodeConfig.setPool((String) this.pool.get());
        }

        binder.bind(NodeConfig.class).toInstance(nodeConfig);
        ExportBinder.newExporter(binder).export(NodeInfo.class).withGeneratedName();
    }

    private static InetAddress getV4Localhost()
    {
        try {
            return InetAddress.getByAddress("localhost", new byte[] {127, 0, 0, 1});
        }
        catch (UnknownHostException var1) {
            throw new AssertionError("Could not create localhost address");
        }
    }
}
