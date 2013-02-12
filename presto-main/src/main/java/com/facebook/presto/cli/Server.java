package com.facebook.presto.cli;

import com.facebook.presto.event.scribe.client.ScribeClientModule;
import com.facebook.presto.event.scribe.payload.ScribeEventModule;
import com.facebook.presto.hive.HiveClientModule;
import com.facebook.presto.hive.PrismHiveClientModule;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.swift.codec.guice.ThriftCodecModule;
import com.facebook.swift.service.guice.ThriftClientModule;
import com.facebook.swift.smc.SmcClientModule;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.command.Command;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.event.client.JsonEventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import org.weakref.jmx.guice.MBeanModule;

@Command(name = "server", description = "Run the server")
public class Server
        implements Runnable
{
    @Override
    public void run()
    {
        Logger log = Logger.get(Server.class);
        Bootstrap app = new Bootstrap(
                new NodeModule(),
                new DiscoveryModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new JmxModule(),
                new JmxHttpModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new JsonEventModule(),
                new ThriftCodecModule(),
                new ThriftClientModule(),
                new ScribeClientModule(),
                new ScribeEventModule(),
                new HiveClientModule(),
                new PrismHiveClientModule(),
                new SmcClientModule(),
                new ServerMainModule());

        try {
            Injector injector = app.strictConfig().initialize();
            injector.getInstance(Announcer.class).start();
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }
}
