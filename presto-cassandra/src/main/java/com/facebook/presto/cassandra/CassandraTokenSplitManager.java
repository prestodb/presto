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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfSplit;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

public class CassandraTokenSplitManager
{
    private final Cassandra.Client client;
    private final int splitSize;
    @SuppressWarnings("rawtypes")
    private final IPartitioner partitioner;

    @Inject
    public CassandraTokenSplitManager(Cassandra.Client client, CassandraClientConfig config)
    {
        this.client = client;
        this.splitSize = config.getSplitSize();
        try {
            this.partitioner =  FBUtilities.newPartitioner(config.getPartitioner());
        }
        catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public List<TokenSplit> getSplits(String ks, String cf) throws IOException
    {
        List<TokenRange> masterRangeNodes = getRangeMap(ks, client);

        // cannonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = new ThreadPoolExecutor(0, 128, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        List<TokenSplit> splits = new ArrayList<TokenSplit>();
        try {
            List<Future<List<TokenSplit>>> splitfutures = new ArrayList<Future<List<TokenSplit>>>();
            for (TokenRange range : masterRangeNodes) {
                // for each range, pick a live owner and ask it to compute bite-sized splits
                splitfutures.add(executor.submit(new SplitCallable(range, ks, cf, splitSize, client, partitioner)));
            }

            // wait until we have all the results back
            for (Future<List<TokenSplit>> futureInputSplits : splitfutures) {
                try {
                    splits.addAll(futureInputSplits.get());
                }
                catch (Exception e) {
                    throw new IOException("Could not get input splits", e);
                }
            }
        }
        finally {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    private List<TokenRange> getRangeMap(String keyspace, Cassandra.Client client) throws IOException
    {
        List<TokenRange> map;
        try {
            map = client.describe_ring(keyspace);
        }
        catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    private List<CfSplit> getSubSplits(String keyspace, String cfName, TokenRange range, int splitsize, Cassandra.Client client) throws IOException
    {
        for (int i = 0; i < range.rpc_endpoints.size(); i++) {
            String host = range.rpc_endpoints.get(i);
            if (host == null || host.equals("0.0.0.0")) {
                host = range.endpoints.get(i);
            }

            try {
                client.set_keyspace(keyspace);
                try {
                    return client.describe_splits_ex(cfName, range.start_token, range.end_token, splitsize);
                }
                catch (TApplicationException e) {
                    // fallback to guessing split size if talking to a server without describe_splits_ex method
                    if (e.getType() == TApplicationException.UNKNOWN_METHOD) {
                        List<String> splitPoints = client.describe_splits(cfName, range.start_token, range.end_token, splitsize);
                        return tokenListToSplits(splitPoints, splitsize);
                    }
                    throw e;
                }
            }
            catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            }
            catch (TException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IOException("failed connecting to all endpoints " + range.endpoints.toString());
    }

    private List<CfSplit> tokenListToSplits(List<String> splitTokens, int splitsize)
    {
        List<CfSplit> splits = Lists.newArrayListWithExpectedSize(splitTokens.size() - 1);
        for (int j = 0; j < splitTokens.size() - 1; j++) {
            splits.add(new CfSplit(splitTokens.get(j), splitTokens.get(j + 1), splitsize));
        }
        return splits;
    }

    /**
     * Gets a token range and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<TokenSplit>>
    {
        private final TokenRange range;
        private final String ks;
        private final String cf;
        private final int splitSize;
        private final Cassandra.Client client;
        private final IPartitioner partitioner;

        public SplitCallable(TokenRange tr, String ks, String cf, int splitSize, Cassandra.Client client, IPartitioner partitioner)
        {
            this.range = tr;
            this.ks = ks;
            this.cf = cf;
            this.splitSize = splitSize;
            this.client = client;
            this.partitioner = partitioner;
        }

        public List<TokenSplit> call() throws Exception
        {
            ArrayList<TokenSplit> splits = new ArrayList<TokenSplit>();
            List<CfSplit> subSplits = getSubSplits(ks, cf, range, splitSize, client);
            assert range.rpc_endpoints.size() == range.endpoints.size() : "rpc_endpoints size must match endpoints size";
            // turn the sub-ranges into InputSplits
            List<String> endpoints = range.endpoints;

            Token.TokenFactory factory = partitioner.getTokenFactory();
            for (CfSplit subSplit : subSplits) {
                Token left = factory.fromString(subSplit.getStart_token());
                Token right = factory.fromString(subSplit.getEnd_token());
                Range<Token> range = new Range<Token>(left, right, partitioner);
                List<Range<Token>> ranges = range.isWrapAround() ? range.unwrap() : ImmutableList.of(range);
                for (Range<Token> subrange : ranges) {
                    TokenSplit split = new TokenSplit(factory.toString(subrange.left), factory.toString(subrange.right), endpoints);
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
            this.startToken = startToken;
            this.endToken = endToken;
            this.hosts = ImmutableList.copyOf(hosts);
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
