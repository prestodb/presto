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

import static com.facebook.presto.cassandra.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import io.airlift.units.Duration;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import org.weakref.jmx.Managed;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Cassandra Schema Cache
 */
@ThreadSafe
public class CachingCassandraSchemaProvider {
	private final CassandraSession session;
	private final LoadingCache<String, List<String>> schemaNamesCache;
	private final LoadingCache<String, List<String>> tableNamesCache;
	private final LoadingCache<PartitionListKey, List<CassandraPartition>> partitionsCache;
	private final LoadingCache<PartitionListKey, List<CassandraPartition>> partitionsCacheFull;
	private final LoadingCache<SchemaTableName, CassandraTable> tableCache;

	@Inject
	public CachingCassandraSchemaProvider(CassandraSession session,
			@ForCassandraSchema ExecutorService executor,
			CassandraClientConfig cassandraClientConfig) {
		this(session, executor,
				checkNotNull(cassandraClientConfig, "cassandraClientConfig is null")
						.getSchemaCacheTtl(), cassandraClientConfig.getSchemaRefreshInterval());
	}

	public CachingCassandraSchemaProvider(CassandraSession session, ExecutorService executor,
			Duration cacheTtl, Duration refreshInterval) {
		this.session = checkNotNull(session, "cassandraSession is null");
		checkNotNull(executor, "executor is null");

		long expiresAfterWriteMillis = checkNotNull(cacheTtl, "cacheTtl is null").toMillis();
		long refreshMills = checkNotNull(refreshInterval, "refreshInterval is null").toMillis();

		ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);

		schemaNamesCache = CacheBuilder.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(new BackgroundCacheLoader<String, List<String>>(listeningExecutor) {
					@Override
					public List<String> load(String key) throws Exception {
						return loadAllSchemas();
					}
				});

		tableNamesCache = CacheBuilder.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(new BackgroundCacheLoader<String, List<String>>(listeningExecutor) {
					@Override
					public List<String> load(String databaseName) throws Exception {
						return loadAllTables(databaseName);
					}
				});

		tableCache = CacheBuilder
				.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(new BackgroundCacheLoader<SchemaTableName, CassandraTable>(listeningExecutor) {
					@Override
					public CassandraTable load(SchemaTableName tableName) throws Exception {
						return loadTable(tableName);
					}
				});

		partitionsCache = CacheBuilder
				.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.refreshAfterWrite(refreshMills, MILLISECONDS)
				.build(new BackgroundCacheLoader<PartitionListKey, List<CassandraPartition>>(
						listeningExecutor) {
					@Override
					public List<CassandraPartition> load(PartitionListKey key)
							throws Exception {
						return loadPartitions(key);
					}
				});
		
		partitionsCacheFull = CacheBuilder
				.newBuilder()
				.expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS)
				.build(new BackgroundCacheLoader<PartitionListKey, List<CassandraPartition>>(
						listeningExecutor) {
					@Override
					public List<CassandraPartition> load(PartitionListKey key)
							throws Exception {
						return loadPartitions(key);
					}
				});
	}

	@Managed
	public void flushCache() {
		schemaNamesCache.invalidateAll();
		tableNamesCache.invalidateAll();
		partitionsCache.invalidateAll();
		tableCache.invalidateAll();
	}

	private static <K, V, E extends Exception> V get(LoadingCache<K, V> cache, K key,
			Class<E> exceptionClass) throws E {
		try {
			return cache.get(key);
		} catch (ExecutionException | UncheckedExecutionException e) {
			Throwable t = e.getCause();
			Throwables.propagateIfInstanceOf(t, exceptionClass);
			throw Throwables.propagate(t);
		}
	}

	public List<String> getAllSchemas() {
		return get(schemaNamesCache, "", RuntimeException.class);
	}

	private List<String> loadAllSchemas() throws Exception {
		return retry().stopOnIllegalExceptions().run("getAllSchemas", new Callable<List<String>>() {
			@Override
			public List<String> call() throws Exception {
				return session.getAllSchemas();
			}
		});
	}

	public List<String> getAllTables(String databaseName) throws NoSuchObjectException {
		return get(tableNamesCache, databaseName, NoSuchObjectException.class);
	}

	private List<String> loadAllTables(final String databaseName) throws Exception {
		return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions()
				.run("getAllTables", new Callable<List<String>>() {
					@Override
					public List<String> call() throws Exception {
						List<String> tables = session.getAllTables(databaseName);
						if (tables.isEmpty()) {
							// Check to see if the database exists
							session.getSchema(databaseName);
						}
						return tables;
					}
				});
	}

	public CassandraTable getTable(CassandraTableHandle tableHandle) throws NoSuchObjectException {
		return get(tableCache, tableHandle.getSchemaTableName(), NoSuchObjectException.class);
	}

	private CassandraTable loadTable(final SchemaTableName tableName) throws Exception {
		return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions()
				.run("getTable", new Callable<CassandraTable>() {
					@Override
					public CassandraTable call() throws Exception {
						CassandraTable table = session.getTable(tableName);
						return table;
					}
				});
	}

	public List<CassandraPartition> getPartitions(CassandraTable table,
			List<Comparable<?>> filterPrefix) throws NoSuchObjectException {
		final LoadingCache<PartitionListKey, List<CassandraPartition>> cache;
		if (filterPrefix.size() == table.getPartitionKeyColumns().size()) {
			cache = partitionsCacheFull;
		} else {
			cache = partitionsCache;
		}
		PartitionListKey key = new PartitionListKey(table, filterPrefix);
		return get(cache, key, NoSuchObjectException.class);
	}

	private List<CassandraPartition> loadPartitions(final PartitionListKey key) throws Exception {
		return retry().stopOn(NoSuchObjectException.class).stopOnIllegalExceptions()
				.run("getPartitions", new Callable<List<CassandraPartition>>() {
					@Override
					public List<CassandraPartition> call() throws Exception {
						return session.getPartitions(key.table, key.filterPrefix);
					}
				});
	}
	
	private static class PartitionListKey {
		final CassandraTable table;
		final List<Comparable<?>> filterPrefix;
		
		PartitionListKey(CassandraTable table, List<Comparable<?>> filterPrefix) {
			this.table = table;
			this.filterPrefix = ImmutableList.copyOf(filterPrefix);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((filterPrefix == null) ? 0 : filterPrefix.hashCode());
			result = prime * result + ((table == null) ? 0 : table.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PartitionListKey other = (PartitionListKey) obj;
			if (filterPrefix == null) {
				if (other.filterPrefix != null)
					return false;
			} else if (!filterPrefix.equals(other.filterPrefix))
				return false;
			if (table == null) {
				if (other.table != null)
					return false;
			} else if (!table.equals(other.table))
				return false;
			return true;
		}
		
		
		
	}
}
