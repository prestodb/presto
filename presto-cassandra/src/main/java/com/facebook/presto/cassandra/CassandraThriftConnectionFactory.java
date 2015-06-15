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
package com.facebook.presto.cassandra;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ITransportFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CassandraThriftConnectionFactory
{
    private static final Logger log = Logger.get(CassandraThriftConnectionFactory.class);
    private final int port;
    private final List<String> addresses;
    private final String factoryClassName;
    private final Map<String, String> transportFactoryOptions;

    @Inject
    public CassandraThriftConnectionFactory(CassandraClientConfig config)
    {
        this.addresses = config.getContactPoints();
        this.port = config.getThriftPort();
        this.factoryClassName = config.getThriftConnectionFactoryClassName();
        this.transportFactoryOptions = ImmutableMap.copyOf(config.getTransportFactoryOptions());
    }

    public Cassandra.Client create()
    {
        try {
            return getClientFromAddressList();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Cassandra.Client getClientFromAddressList() throws IOException
    {
        List<IOException> exceptions = new ArrayList<IOException>();
        // randomly select host to connect to
        List<String> addresses = new ArrayList<>(this.addresses);
        Collections.shuffle(addresses);
        for (String address : addresses) {
            try {
                return createConnection(address, port, factoryClassName);
            }
            catch (IOException ioe) {
                exceptions.add(ioe);
            }
        }
        log.error("failed to connect to any initial addresses");
        for (IOException exception : exceptions) {
            log.error(exception);
        }
        throw exceptions.get(exceptions.size() - 1);
    }

    public Cassandra.Client createConnection(String host, Integer port, String factoryClassName) throws IOException
    {
        try {
            TTransport transport = getClientTransportFactory(factoryClassName).openTransport(host, port);
            return new Cassandra.Client(new TBinaryProtocol(transport, true, true));
        }
        catch (Exception e) {
            throw new IOException("Unable to connect to server " + host + ":" + port, e);
        }
    }

    private ITransportFactory getClientTransportFactory(String factoryClassName)
    {
        try {
            ITransportFactory factory = (ITransportFactory) Class.forName(factoryClassName).newInstance();
            Map<String, String> options = getOptions(factory.supportedOptions());
            factory.setOptions(options);
            return factory;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to instantiate transport factory:" + factoryClassName, e);
        }
    }

    private Map<String, String> getOptions(Set<String> supportedOptions)
    {
        Map<String, String> options = new HashMap<>();
        for (String optionKey : supportedOptions) {
            String optionValue = transportFactoryOptions.get(optionKey);
            if (optionValue != null) {
                options.put(optionKey, optionValue);
            }
        }
        return options;
    }
}
