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
package com.facebook.presto.hive.thrift;

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class TTransportPool
{
    private static final int MIN_IDLE = 0;
    private static final int MAX_TOTAL = 128;
    private static final int NUM_TESTS = 3;
    private static final Duration IDLE_TIMEOUT = new Duration(300, TimeUnit.SECONDS);
    private static final Duration EVICTION_INTERVEL = new Duration(10, TimeUnit.SECONDS);
    private final ConcurrentMap<String, ObjectPool<TTransport>> pools = new ConcurrentHashMap();
    private GenericObjectPoolConfig poolConfig;

    public TTransportPool()
    {
        poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(MAX_TOTAL);
        poolConfig.setMinIdle(MIN_IDLE);
        poolConfig.setMaxTotal(MAX_TOTAL);
        poolConfig.setMinEvictableIdleTimeMillis(IDLE_TIMEOUT.toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(EVICTION_INTERVEL.toMillis());
        poolConfig.setNumTestsPerEvictionRun(NUM_TESTS);
    }

    public TTransportPool(GenericObjectPoolConfig poolConfig)
    {
        this.poolConfig = poolConfig;
    }

    private void add(String remote, PooledObjectFactory transportFactory)
    {
        pools.put(remote, new GenericObjectPool<TTransport>(transportFactory, poolConfig));
    }

    protected TTransport get(String remote, PooledObjectFactory transportFactory)
        throws Exception
    {
        ObjectPool<TTransport> pool = pools.get(remote);
        if (pool == null) {
            add(remote, transportFactory);
            pool = pools.get(remote);
        }
        return pool.borrowObject();
    }

    protected TTransport get(String remote)
        throws Exception
    {
        ObjectPool<TTransport> pool = pools.get(remote);
        if (pool == null) {
            return null;
        }
        return pool.borrowObject();
    }

    public TTransport borrowObject(String host, int port, PooledObjectFactory transportFactory)
        throws Exception
    {
        return get(HostAndPort.fromParts(host, port).toString(), transportFactory);
    }

    public TTransport borrowObject(String host, int port)
        throws Exception
    {
        return get(HostAndPort.fromParts(host, port).toString());
    }

    public void returnObject(String remote, TTransport pooledTransport, TTransport transport)
    {
        if (remote == null) {
            transport.close();
            return;
        }
        ObjectPool<TTransport> pool = pools.get(remote);
        if (pool == null) {
            transport.close();
            return;
        }
        try {
            pool.returnObject(pooledTransport);
        }
        catch (Exception e) {
            transport.close();
        }
    }

    public void returnObject(TTransport transport)
    {
        transport.close();
    }
}
