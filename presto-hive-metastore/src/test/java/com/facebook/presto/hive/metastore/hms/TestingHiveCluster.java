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
package com.facebook.presto.hive.metastore.hms;

import com.facebook.presto.hive.HiveCommonClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.hms.http.HttpHiveMetastoreClientFactory;
import com.facebook.presto.hive.metastore.hms.http.HttpHiveMetastoreConfig;
import com.facebook.presto.hive.metastore.hms.thrift.ThriftHiveMetastoreClientFactory;
import com.facebook.presto.hive.metastore.hms.thrift.ThriftHiveMetastoreConfig;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingHiveCluster
        implements HiveCluster
{
    private final MetastoreClientConfig metastoreClientConfig;
    private final ThriftHiveMetastoreConfig thriftHiveMetastoreConfig;
    private final HttpHiveMetastoreConfig httpHiveMetastoreConfig;
    private final URI uri;
    private boolean httpEnabled;
    private final HiveCommonClientConfig hiveCommonClientConfig;

    public TestingHiveCluster(MetastoreClientConfig metastoreClientConfig, ThriftHiveMetastoreConfig thriftHiveMetastoreConfig, HttpHiveMetastoreConfig httpHiveMetastoreConfig, String host, int port, HiveCommonClientConfig hiveCommonClientConfig, StaticMetastoreConfig staticMetastoreConfig)
    {
        this.metastoreClientConfig = requireNonNull(metastoreClientConfig, "metastore config is null");
        this.thriftHiveMetastoreConfig = requireNonNull(thriftHiveMetastoreConfig, "thrift metastore config is null");
        this.httpHiveMetastoreConfig = requireNonNull(httpHiveMetastoreConfig, "http metastore conf is null");

        this.httpEnabled = Optional.ofNullable(staticMetastoreConfig.getMetastoreUris())
                .orElse(Collections.emptyList())
                .stream()
                .map(URI::getScheme)
                .filter(Objects::nonNull)
                .anyMatch(scheme -> "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme));

        String scheme = httpEnabled ? "http" : "thrift";
        String path = httpEnabled ? "/metastore" : "";

        this.uri = URI.create(String.format("%s://%s:%d%s", scheme, requireNonNull(host, "host is null"), port, path));
        this.hiveCommonClientConfig = hiveCommonClientConfig;
    }

    @Override
    public HiveMetastoreClient createMetastoreClient(Optional<String> token)
            throws TException
    {
        return (httpEnabled) ? new HttpHiveMetastoreClientFactory(httpHiveMetastoreConfig, hiveCommonClientConfig).create(uri, token)
                             : new ThriftHiveMetastoreClientFactory(metastoreClientConfig, thriftHiveMetastoreConfig, new NoHiveMetastoreAuthentication(), hiveCommonClientConfig).create(uri, token);
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

        return Objects.equals(this.uri, o.uri);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uri);
    }
}
