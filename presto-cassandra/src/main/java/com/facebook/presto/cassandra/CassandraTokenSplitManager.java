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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.dht.Token.TokenFactory;

public class CassandraTokenSplitManager
{
    private final Cassandra.Client client;
    private final ExecutorService executor;
    private final int splitSize;
    private final IPartitioner<?> partitioner;

    @Inject
    public CassandraTokenSplitManager(Cassandra.Client client, @ForCassandra ExecutorService executor, CassandraClientConfig config)
    {
        this.client = checkNotNull(client, "client is null");
        this.executor = checkNotNull(executor, "executor is null");
        this.splitSize = checkNotNull(config, "config is null").getSplitSize();
        try {
            this.partitioner = FBUtilities.newPartitioner(config.getPartitioner());
        }
        catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public List<TokenSplit> getSplits(String keyspace, String columnFamily)
            throws IOException
    {
        List<TokenRange> masterRangeNodes = getRangeMap(keyspace, client);

        // canonical ranges, split into pieces, fetching the splits in parallel
        List<TokenSplit> splits = new ArrayList<>();
        List<Future<List<TokenSplit>>> splitFutures = new ArrayList<>();
        for (TokenRange range : masterRangeNodes) {
            // for each range, pick a live owner and ask it to compute bite-sized splits
            splitFutures.add(executor.submit(new SplitCallable<>(range, keyspace, columnFamily, splitSize, client, partitioner)));
        }

        // wait until we have all the results back
        for (Future<List<TokenSplit>> futureInputSplits : splitFutures) {
            try {
                splits.addAll(futureInputSplits.get());
            }
            catch (Exception e) {
                throw new IOException("Could not get input splits", e);
            }
        }

        checkState(!splits.isEmpty(), "No splits created");
        Collections.shuffle(splits, ThreadLocalRandom.current());
        return splits;
    }

    private static List<TokenRange> getRangeMap(String keyspace, Cassandra.Client client)
            throws IOException
    {
        try {
            return client.describe_ring(keyspace);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<CfSplit> getSubSplits(String keyspace, String columnFamily, TokenRange range, int splitSize, Cassandra.Client client)
            throws IOException
    {
        try {
            client.set_keyspace(keyspace);
            try {
                return client.describe_splits_ex(columnFamily, range.start_token, range.end_token, splitSize);
            }
            catch (TApplicationException e) {
                // fallback to guessing split size if talking to a server without describe_splits_ex method
                if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                    List<String> splitPoints = client.describe_splits(columnFamily, range.start_token, range.end_token, splitSize);
                    return tokenListToSplits(splitPoints, splitSize);
                }
                throw e;
            }
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<CfSplit> tokenListToSplits(List<String> splitTokens, int splitSize)
    {
        ImmutableList.Builder<CfSplit> splits = ImmutableList.builder();
        for (int index = 0; index < splitTokens.size() - 1; index++) {
            splits.add(new CfSplit(splitTokens.get(index), splitTokens.get(index + 1), splitSize));
        }
        return splits.build();
    }

    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    private class SplitCallable<T extends Token<?>>
            implements Callable<List<TokenSplit>>
    {
        private final TokenRange range;
        private final String keyspace;
        private final String columnFamily;
        private final int splitSize;
        private final Cassandra.Client client;
        private final IPartitioner<T> partitioner;

        public SplitCallable(TokenRange range, String keyspace, String columnFamily, int splitSize, Cassandra.Client client, IPartitioner<T> partitioner)
        {
            checkArgument(range.rpc_endpoints.size() == range.endpoints.size(), "rpc_endpoints size must match endpoints size");
            this.range = range;
            this.keyspace = keyspace;
            this.columnFamily = columnFamily;
            this.splitSize = splitSize;
            this.client = client;
            this.partitioner = partitioner;
        }

        @Override
        public List<TokenSplit> call()
                throws Exception
        {
            ArrayList<TokenSplit> splits = new ArrayList<>();
            List<CfSplit> subSplits = getSubSplits(keyspace, columnFamily, range, splitSize, client);

            // turn the sub-ranges into InputSplits
            List<String> endpoints = range.endpoints;
            TokenFactory<T> factory = partitioner.getTokenFactory();
            for (CfSplit subSplit : subSplits) {
                Token<T> left = factory.fromString(subSplit.getStart_token());
                Token<T> right = factory.fromString(subSplit.getEnd_token());
                Range<Token<T>> range = new Range<>(left, right, partitioner);
                List<Range<Token<T>>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);
                for (Range<Token<T>> subRange : ranges) {
                    TokenSplit split = new TokenSplit(factory.toString(subRange.left), factory.toString(subRange.right), endpoints);

                    splits.add(split);
                }
            }
            return splits;
        }
    }

    public static class TokenSplit
    {
        private String startToken;
        private String endToken;
        private List<String> hosts;

        public TokenSplit(String startToken, String endToken, List<String> hosts)
        {
            this.startToken = checkNotNull(startToken, "startToken is null");
            this.endToken = checkNotNull(endToken, "endToken is null");
            this.hosts = ImmutableList.copyOf(checkNotNull(hosts, "hosts is null"));
        }

        public String getStartToken()
        {
            return startToken;
        }

        public String getEndToken()
        {
            return endToken;
        }

        public List<String> getHosts()
        {
            return hosts;
        }
    }
}
