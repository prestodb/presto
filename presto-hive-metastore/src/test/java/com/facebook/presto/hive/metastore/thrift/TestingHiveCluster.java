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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHiveMetastoreAuthentication;
import com.google.common.net.HostAndPort;
import org.apache.thrift.TException;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingHiveCluster
        implements HiveCluster
{
    private final MetastoreClientConfig metastoreClientConfig;
    private final HostAndPort address;

    public TestingHiveCluster(MetastoreClientConfig metastoreClientConfig, String host, int port)
    {
        this.metastoreClientConfig = requireNonNull(metastoreClientConfig, "metastore config is null");
        this.address = HostAndPort.fromParts(requireNonNull(host, "host is null"), port);
    }

    @Override
    public HiveMetastoreClient createMetastoreClient(Optional<String> token)
            throws TException
    {
        return new HiveMetastoreClientFactory(metastoreClientConfig, new NoHiveMetastoreAuthentication()).create(address, token);
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

        return Objects.equals(this.address, o.address);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(address);
    }
}
