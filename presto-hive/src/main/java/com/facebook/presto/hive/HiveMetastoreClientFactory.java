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
import com.facebook.hive.metastore.api.ThriftHiveMetastore.Client;
import com.facebook.hive.metastore.client.HiveMetastoreFactory;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class HiveMetastoreClientFactory
{
    private final HiveMetastoreFactory hiveMetastoreFactory;

    @Inject
    public HiveMetastoreClientFactory(HiveMetastoreFactory hiveMetastoreFactory)
    {
        this.hiveMetastoreFactory = checkNotNull(hiveMetastoreFactory, "hiveMetastoreFactory is null");
    }

    public Client create(Set<HostAndPort> hostAndPorts)
    {
        return ThriftHiveMetastore.Client.forHiveMetastore(hiveMetastoreFactory.getClientForHost(hostAndPorts));
    }
}
