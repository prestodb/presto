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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import javax.inject.Inject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

public class CassandraSessionFactory {
	private final List<String> contactPoints;
	private final CassandraConnectorId connectorId;
	private final CassandraClientConfig config;

	@Inject
	public CassandraSessionFactory(CassandraConnectorId connectorId, CassandraClientConfig config) {
		this.connectorId = checkNotNull(connectorId, "connectorId is null");
		this.contactPoints = checkNotNull(config.getContactPoints(), "contactPoints is null");
		checkArgument(contactPoints.size() > 0, "empty contactPoints");
		this.config = config;
	}

	public CassandraSession create() {
		if (connectorId.toString().endsWith("-simulate")) {
			// load mock class for testing without Cassandra server
			try {
				Class<?> cls = Class.forName("com.facebook.presto.cassandra.MockCassandraSession");
				return (CassandraSession) cls.getConstructor(String.class).newInstance(connectorId.toString());
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		} else {
			final Builder builder = Cluster.builder();
			builder.addContactPoints(contactPoints.toArray(new String[0]));
			QueryOptions options = new QueryOptions();
			options.setFetchSize(config.getFetchSize());
			ConsistencyLevel level = ConsistencyLevel.valueOf(config.getConsistencyLevel());
			checkNotNull(level, "Invalid consistency level: " + config.getConsistencyLevel());
			options.setConsistencyLevel(level);
			Cluster cluster = builder.withQueryOptions(options).build();
			Session session = cluster.connect();
			return new CassandraSession(connectorId.toString(), session, config);
		}
	}
}
