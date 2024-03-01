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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.TokenRange;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.cassandra.CassandraErrorCode.CASSANDRA_METADATA_ERROR;
import static com.facebook.presto.cassandra.TokenRing.createForPartitioner;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.lang.Math.round;
import static java.lang.StrictMath.toIntExact;
import static java.util.Collections.shuffle;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CassandraTokenSplitManager
{
    private final CassandraSession session;
    private final int splitSize;
    private final Optional<Long> configSplitsPerNode;

    @Inject
    public CassandraTokenSplitManager(CassandraSession session, CassandraClientConfig config)
    {
        this(session, config.getSplitSize(), config.getSplitsPerNode());
    }

    public CassandraTokenSplitManager(CassandraSession session, int splitSize, Optional<Long> configSplitsPerNode)
    {
        this.session = requireNonNull(session, "session is null");
        this.splitSize = splitSize;
        this.configSplitsPerNode = configSplitsPerNode;
    }

    public List<TokenSplit> getSplits(String keyspace, String table, Optional<Long> sessionSplitsPerNode)
    {
        Set<TokenRange> tokenRanges = session.getTokenRanges();

        if (tokenRanges.isEmpty()) {
            throw new PrestoException(CASSANDRA_METADATA_ERROR, "The cluster metadata is not available. " +
                    "Please make sure that the Cassandra cluster is up and running, " +
                    "and that the contact points are specified correctly.");
        }

        if (tokenRanges.stream().anyMatch(TokenRange::isWrappedAround)) {
            tokenRanges = unwrap(tokenRanges);
        }

        Optional<TokenRing> tokenRing = createForPartitioner(session.getPartitioner());
        long totalPartitionsCount = getTotalPartitionsCount(keyspace, table, sessionSplitsPerNode);

        List<TokenSplit> splits = new ArrayList<>();
        for (TokenRange tokenRange : tokenRanges) {
            if (tokenRange.isEmpty()) {
                continue;
            }
            checkState(!tokenRange.isWrappedAround(), "all token ranges must be unwrapped at this step");

            List<String> endpoints = getEndpoints(keyspace, tokenRange);
            checkState(!endpoints.isEmpty(), "endpoints is empty for token range: %s", tokenRange);

            if (!tokenRing.isPresent()) {
                checkState(!tokenRange.isWrappedAround(), "all token ranges must be unwrapped at this step");
                splits.add(createSplit(tokenRange, endpoints));
                continue;
            }

            double tokenRangeRingFraction = tokenRing.get().getRingFraction(tokenRange.getStart().toString(), tokenRange.getEnd().toString());
            long partitionsCountEstimate = round(totalPartitionsCount * tokenRangeRingFraction);
            checkState(partitionsCountEstimate >= 0, "unexpected partitions count estimate: %d", partitionsCountEstimate);
            int subSplitCount = max(toIntExact(partitionsCountEstimate / splitSize), 1);
            List<TokenRange> subRanges = tokenRange.splitEvenly(subSplitCount);

            for (TokenRange subRange : subRanges) {
                if (subRange.isEmpty()) {
                    continue;
                }
                checkState(!subRange.isWrappedAround(), "all token ranges must be unwrapped at this step");
                splits.add(createSplit(subRange, endpoints));
            }
        }
        shuffle(splits, ThreadLocalRandom.current());
        return unmodifiableList(splits);
    }

    private Set<TokenRange> unwrap(Set<TokenRange> tokenRanges)
    {
        ImmutableSet.Builder<TokenRange> result = ImmutableSet.builder();
        for (TokenRange range : tokenRanges) {
            result.addAll(range.unwrap());
        }
        return result.build();
    }

    public long getTotalPartitionsCount(String keyspace, String table, Optional<Long> sessionSplitsPerNode)
    {
        if (sessionSplitsPerNode.isPresent()) {
            return sessionSplitsPerNode.get();
        }
        else if (configSplitsPerNode.isPresent()) {
            return configSplitsPerNode.get();
        }
        List<SizeEstimate> estimates = session.getSizeEstimates(keyspace, table);
        return estimates.stream()
                .mapToLong(SizeEstimate::getPartitionsCount)
                .sum();
    }

    private List<String> getEndpoints(String keyspace, TokenRange tokenRange)
    {
        Set<Host> endpoints = session.getReplicas(keyspace, tokenRange);
        return unmodifiableList(endpoints.stream()
                .map(Host::toString)
                .collect(toList()));
    }

    private static TokenSplit createSplit(TokenRange range, List<String> endpoints)
    {
        checkArgument(!range.isEmpty(), "tokenRange must not be empty");
        String startToken = range.getStart().toString();
        String endToken = range.getEnd().toString();
        return new TokenSplit(startToken, endToken, endpoints);
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
