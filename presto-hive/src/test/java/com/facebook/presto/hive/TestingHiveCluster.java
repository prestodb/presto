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
package com.facebook.presto.hive;

import com.facebook.hive.metastore.api.ThriftHiveMetastore;
import com.facebook.hive.metastore.client.HiveMetastoreClientConfig;
import com.facebook.hive.metastore.client.HiveMetastoreFactory;
import com.facebook.hive.metastore.client.SimpleHiveMetastoreFactory;
import com.facebook.swift.service.ThriftClientConfig;
import com.facebook.swift.service.ThriftClientManager;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;

import java.io.Closeable;

import static com.google.common.base.Preconditions.checkNotNull;

public class TestingHiveCluster
        implements HiveCluster, Closeable
{
    private final String host;
    private final int port;

    private final HiveMetastoreClientConfig hiveMetastoreClientConfig = new HiveMetastoreClientConfig();
    private final ThriftClientManager thriftClientManager;
    private final HiveMetastoreFactory factory;

    public TestingHiveCluster(String host, int port)
    {
        this.host = checkNotNull(host, "host is null");
        this.port = port;
        this.thriftClientManager = new ThriftClientManager();
        this.factory = new SimpleHiveMetastoreFactory(thriftClientManager, new ThriftClientConfig(), hiveMetastoreClientConfig);
    }

    @Override
    public void close()
    {
        thriftClientManager.close();
    }

    @Override
    public ThriftHiveMetastore createMetastoreClient()
    {
        return new HiveMetastoreClientFactory(factory).create(ImmutableSet.of(HostAndPort.fromParts(host, port)));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TestingHiveCluster o = (TestingHiveCluster) obj;

        return Objects.equal(this.host, o.host) &&
                Objects.equal(this.port, o.port);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(host, port);
    }
}
