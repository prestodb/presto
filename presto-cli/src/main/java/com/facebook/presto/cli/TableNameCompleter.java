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
package com.facebook.presto.cli;

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import jline.console.completer.Completer;

import java.io.Closeable;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TableNameCompleter
        implements Completer, Closeable
{
    private static final long RELOAD_TIME_MINUTES = 2;

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("completer-%s"));
    private final QueryRunner queryRunner;
    private final LoadingCache<String, List<String>> tableCache;
    private final LoadingCache<String, List<String>> functionCache;

    public TableNameCompleter(QueryRunner queryRunner)
    {
        this.queryRunner = checkNotNull(queryRunner, "queryRunner session was null!");

        ListeningExecutorService listeningExecutor = MoreExecutors.listeningDecorator(executor);
        tableCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(RELOAD_TIME_MINUTES, TimeUnit.MINUTES)
                .build(new BackgroundCacheLoader<String, List<String>>(listeningExecutor)
                {
                    @Override
                    public List<String> load(String schemaName)
                    {
                        return queryMetadata(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName));
                    }
                });
        functionCache = CacheBuilder.newBuilder()
                .build(new BackgroundCacheLoader<String, List<String>>(listeningExecutor)
                {
                    @Override
                    public List<String> load(String schemaName)
                    {
                        return queryMetadata("SHOW FUNCTIONS");
                    }
                });
    }

    private List<String> queryMetadata(String query)
    {
        ImmutableList.Builder<String> cache = ImmutableList.builder();
        try (StatementClient client = queryRunner.startInternalQuery(query)) {
            while (client.isValid() && !Thread.currentThread().isInterrupted()) {
                QueryResults results = client.current();
                if (results.getData() != null) {
                    for (List<Object> row : results.getData()) {
                        cache.add((String) row.get(0));
                    }
                }
                client.advance();
            }
        }
        return cache.build();
    }

    public void populateCache()
    {
        final String schemaName = queryRunner.getSession().getSchema();
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                functionCache.refresh(schemaName);
                tableCache.refresh(schemaName);
            }
        });
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (cursor <= 0) {
            return cursor;
        }
        int blankPos = findLastBlank(buffer.substring(0, cursor));
        String prefix = buffer.substring(blankPos + 1, cursor);
        String schemaName = queryRunner.getSession().getSchema();
        List<String> functionNames = functionCache.getIfPresent(schemaName);
        List<String> tableNames = tableCache.getIfPresent(schemaName);

        SortedSet<String> sortedCandidates = new TreeSet<>();
        if (functionNames != null) {
            sortedCandidates.addAll(filterResults(functionNames, prefix));
        }
        if (tableNames != null) {
            sortedCandidates.addAll(filterResults(tableNames, prefix));
        }

        candidates.addAll(sortedCandidates);
        return blankPos + 1;
    }

    private static int findLastBlank(String buffer)
    {
        for (int i = buffer.length() - 1; i >= 0; i--) {
            if (Character.isWhitespace(buffer.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static List<String> filterResults(List<String> values, String prefix)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String value : values) {
            if (value.startsWith(prefix)) {
                builder.add(value);
            }
        }
        return builder.build();
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
    }

    private abstract static class BackgroundCacheLoader<K, V>
            extends CacheLoader<K, V>
    {
        private final ListeningExecutorService executor;

        protected BackgroundCacheLoader(ListeningExecutorService executor)
        {
            this.executor = checkNotNull(executor, "executor is null");
        }

        @Override
        public final ListenableFuture<V> reload(final K key, V oldValue)
        {
            return executor.submit(new Callable<V>()
            {
                @Override
                public V call()
                        throws Exception
                {
                    return load(key);
                }
            });
        }
    }
}
