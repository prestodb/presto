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
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.FBUtilities;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.cassandra.dht.Token.TokenFactory;

public class CassandraTokenSplitManager
{
    private final CassandraThriftClient cassandraThriftClient;
    private final ExecutorService executor;
    private final int splitSize;
    private final IPartitioner partitioner;

    @Inject
    public CassandraTokenSplitManager(CassandraThriftConnectionFactory connectionFactory, @ForCassandra ExecutorService executor, CassandraClientConfig config)
    {
        this.cassandraThriftClient = new CassandraThriftClient(requireNonNull(connectionFactory, "connectionFactory is null"));
        this.executor = requireNonNull(executor, "executor is null");
        this.splitSize = config.getSplitSize();
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
        List<TokenRange> masterRangeNodes = cassandraThriftClient.getRangeMap(keyspace);

        // canonical ranges, split into pieces, fetching the splits in parallel
        List<TokenSplit> splits = new ArrayList<>();
        List<Future<List<TokenSplit>>> splitFutures = new ArrayList<>();
        for (TokenRange range : masterRangeNodes) {
            // for each range, pick a live owner and ask it to compute bite-sized splits
            splitFutures.add(executor.submit(new SplitCallable<>(range, keyspace, columnFamily, splitSize, cassandraThriftClient, partitioner)));
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
        //noinspection SharedThreadLocalRandom
        Collections.shuffle(splits, ThreadLocalRandom.current());
        return splits;
    }

    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    private class SplitCallable<T extends Token>
            implements Callable<List<TokenSplit>>
    {
        private final TokenRange range;
        private final String keyspace;
        private final String columnFamily;
        private final int splitSize;
        private final CassandraThriftClient client;
        private final IPartitioner partitioner;

        public SplitCallable(TokenRange range, String keyspace, String columnFamily, int splitSize, CassandraThriftClient client, IPartitioner partitioner)
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
            List<CfSplit> subSplits = client.getSubSplits(keyspace, columnFamily, range, splitSize);

            // turn the sub-ranges into InputSplits
            List<String> endpoints = range.endpoints;
            TokenFactory factory = partitioner.getTokenFactory();
            for (CfSplit subSplit : subSplits) {
                Token left = factory.fromString(subSplit.getStart_token());
                Token right = factory.fromString(subSplit.getEnd_token());
                Range<Token> range = new Range<>(left, right, partitioner);
                List<Range<Token>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);
                for (Range<Token> subRange : ranges) {
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
            this.startToken = requireNonNull(startToken, "startToken is null");
            this.endToken = requireNonNull(endToken, "endToken is null");
            this.hosts = ImmutableList.copyOf(requireNonNull(hosts, "hosts is null"));
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
