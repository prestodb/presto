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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraSessionFactory
{
    private final CassandraConnectorId connectorId;
    private final List<String> contactPoints;

    private final int fetchSize;
    private final ConsistencyLevel consistencyLevel;
    private final int fetchSizeForPartitionKeySelect;
    private final int limitForPartitionKeySelect;
    private final int nativeProtocolPort;
    private final String authUsername;
    private final String authPassword;

    @Inject
    public CassandraSessionFactory(CassandraConnectorId connectorId, CassandraClientConfig config)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        checkNotNull(config, "config is null");

        this.contactPoints = checkNotNull(config.getContactPoints(), "contactPoints is null");
        checkArgument(!contactPoints.isEmpty(), "empty contactPoints");

        nativeProtocolPort = config.getNativeProtocolPort();
        fetchSize = config.getFetchSize();
        consistencyLevel = config.getConsistencyLevel();
        fetchSizeForPartitionKeySelect = config.getFetchSizeForPartitionKeySelect();
        limitForPartitionKeySelect = config.getLimitForPartitionKeySelect();
        authUsername = config.getAuthUsername();
        authPassword = config.getAuthPassword();
    }

    public CassandraSession create()
    {
        Cluster.Builder clusterBuilder = Cluster.builder();
        clusterBuilder.addContactPoints(contactPoints.toArray(new String[contactPoints.size()]));
        clusterBuilder.withPort(nativeProtocolPort);
        clusterBuilder.withReconnectionPolicy(new ExponentialReconnectionPolicy(500, 10000));

        if (authUsername != null && authPassword != null) {
            clusterBuilder.withCredentials(authUsername, authPassword);
        }

        QueryOptions options = new QueryOptions();
        options.setFetchSize(fetchSize);
        options.setConsistencyLevel(consistencyLevel);
        clusterBuilder.withQueryOptions(options);

        return new CassandraSession(connectorId.toString(), clusterBuilder, fetchSizeForPartitionKeySelect, limitForPartitionKeySelect);
    }
}
