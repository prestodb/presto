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
package com.facebook.presto.hive.metastore.hms.http;

import com.facebook.presto.hive.HiveCommonClientConfig;
import com.facebook.presto.hive.metastore.hms.HiveMetastoreClient;
import org.apache.thrift.transport.TTransportException;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MockHttpHiveMetastoreClientFactory
        extends HttpHiveMetastoreClientFactory
{
    private final List<HiveMetastoreClient> clients;

    public MockHttpHiveMetastoreClientFactory(List<HiveMetastoreClient> clients)
    {
        super(new HttpHiveMetastoreConfig(), new HiveCommonClientConfig());
        this.clients = new ArrayList<>(requireNonNull(clients, "clients is null"));
    }

    @Override
    public HiveMetastoreClient create(URI uri, Optional<String> token)
            throws TTransportException
    {
        checkState(!clients.isEmpty(), "mock not given enough clients");
        HiveMetastoreClient client = clients.remove(0);
        if (client == null) {
            throw new TTransportException(TTransportException.TIMED_OUT);
        }
        return client;
    }
}
