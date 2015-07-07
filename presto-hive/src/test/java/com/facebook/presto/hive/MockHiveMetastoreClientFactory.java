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

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;

import java.util.List;

public class MockHiveMetastoreClientFactory
        extends HiveMetastoreClientFactory
{
    private final List<HiveMetastoreClient> toReturn;

    public MockHiveMetastoreClientFactory(@Nullable HostAndPort socksProxy, Duration timeout, List<HiveMetastoreClient> toReturn)
    {
        super(socksProxy, timeout);
        this.toReturn = toReturn;
    }

    public HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        if (toReturn.size() == 0) {
            throw new IllegalArgumentException("Mock not given enough values for the client.");
        }

        HiveMetastoreClient client = toReturn.remove(0);

        if (client == null) {
            throw new TTransportException(TTransportException.TIMED_OUT);
        }
        else {
            return client;
        }
    }
}
