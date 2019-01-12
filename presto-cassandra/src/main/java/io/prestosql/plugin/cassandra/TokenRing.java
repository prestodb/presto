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
package io.prestosql.plugin.cassandra;

import java.math.BigInteger;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface TokenRing
{
    /**
     * Returns ring fraction (value between 0 and 1) represented by the token range
     *
     * @param startToken exclusive
     * @param endToken inclusive
     */
    double getRingFraction(String startToken, String endToken);

    /**
     * Returns token count in a given range
     */
    BigInteger getTokenCountInRange(String startToken, String endToken);

    static Optional<TokenRing> createForPartitioner(String partitioner)
    {
        requireNonNull(partitioner, "partitioner is null");
        switch (partitioner) {
            case "org.apache.cassandra.dht.Murmur3Partitioner":
                return Optional.of(Murmur3PartitionerTokenRing.INSTANCE);
            case "org.apache.cassandra.dht.RandomPartitioner":
                return Optional.of(RandomPartitionerTokenRing.INSTANCE);
            default:
                return Optional.empty();
        }
    }
}
