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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.config.KeyColumn;
import org.jdbi.v3.sqlobject.config.ValueColumn;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.raptorx.util.DatabaseUtil.verifyMetadata;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class NodeIdCache
{
    private final NodeDao dao;

    private final LoadingCache<String, Long> cache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .build(new CacheLoader<String, Long>()
            {
                @Override
                public Long load(String identifier)
                {
                    return loadAll(ImmutableSet.of(identifier)).get(identifier);
                }

                @Override
                public Map<String, Long> loadAll(Iterable<? extends String> identifiers)
                {
                    return loadNodeIds(ImmutableSet.copyOf(identifiers));
                }
            });

    @Inject
    public NodeIdCache(Database database)
    {
        this.dao = createNodeDao(database);
    }

    public long getNodeId(String identifier)
    {
        return getOnlyElement(getNodeIds(ImmutableSet.of(identifier)).values());
    }

    public Map<String, Long> getNodeIds(Set<String> identifiers)
    {
        try {
            return cache.getAll(identifiers);
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    private Map<String, Long> loadNodeIds(Set<String> identifiers)
    {
        Map<String, Long> map = new HashMap<>(dao.getNodeIds(identifiers.stream()
                .map(value -> value.getBytes(UTF_8))
                .collect(toList())));

        for (String identifier : difference(identifiers, map.keySet()).immutableCopy()) {
            dao.insertNode(identifier.getBytes(UTF_8));

            Long id = dao.getNodeId(identifier.getBytes(UTF_8));
            verifyMetadata(id != null, "Node does not exist after insert");
            map.put(identifier, id);
        }

        return map;
    }

    private static NodeDao createNodeDao(Database database)
    {
        Jdbi dbi = createJdbi(database.getMasterConnection());
        switch (database.getType()) {
            case H2:
            case MYSQL:
                return dbi.onDemand(MySqlNodeDao.class);
            case POSTGRESQL:
                return dbi.onDemand(PostgreSqlNodeDao.class);
        }
        throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Unhandled database: " + database.getType());
    }

    public interface NodeDao
    {
        void insertNode(byte[] identifier);

        @SqlQuery("SELECT node_id FROM nodes WHERE identifier = :identifier")
        Long getNodeId(@Bind byte[] identifier);

        @SqlQuery("SELECT identifier, node_id FROM nodes WHERE identifier IN (<identifiers>)")
        @KeyColumn("identifier")
        @ValueColumn("node_id")
        Map<String, Long> getNodeIds(@BindList List<byte[]> identifiers);
    }

    public interface MySqlNodeDao
            extends NodeDao
    {
        @Override
        @SqlUpdate("INSERT IGNORE INTO nodes (identifier) VALUES (:identifier)")
        void insertNode(@Bind byte[] identifier);
    }

    public interface PostgreSqlNodeDao
            extends NodeDao
    {
        @Override
        @SqlUpdate("INSERT INTO nodes (identifier) VALUES (:identifier) ON CONFLICT DO NOTHING")
        void insertNode(@Bind byte[] identifier);
    }
}
