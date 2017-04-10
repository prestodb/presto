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
package com.facebook.presto.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HashCollisionsInfo
        implements Mergeable<HashCollisionsInfo>, OperatorInfo
{
    public static final String WEIGHTED_HASH_COLLISIONS_PROPERTY = "weightedHashCollisions";
    public static final String WEIGHTED_SUM_SQUARED_HASH_COLLISIONS = "weightedSumSquaredHashCollisions";
    public static final String WEIGHTED_EXPECTED_HASH_COLLISIONS = "weightedExpectedHashCollisions";

    private final double weightedHashCollisions;
    private final double weightedSumSquaredHashCollisions;
    private final double weightedExpectedHashCollisions;

    public static HashCollisionsInfo createHashCollisionsInfo(
            long inputPositionsCount, double hashCollisionsCount, double expectedHashCollisions)
    {
        return new HashCollisionsInfo(
                hashCollisionsCount * inputPositionsCount,
                hashCollisionsCount * hashCollisionsCount * inputPositionsCount,
                expectedHashCollisions * inputPositionsCount);
    }

    @JsonCreator
    public HashCollisionsInfo(
            @JsonProperty(WEIGHTED_HASH_COLLISIONS_PROPERTY) double weightedHashCollisions,
            @JsonProperty(WEIGHTED_SUM_SQUARED_HASH_COLLISIONS) double weightedSumSquaredHashCollisions,
            @JsonProperty(WEIGHTED_EXPECTED_HASH_COLLISIONS) double weightedExpectedHashCollisions)
    {
        this.weightedHashCollisions = weightedHashCollisions;
        this.weightedSumSquaredHashCollisions = weightedSumSquaredHashCollisions;
        this.weightedExpectedHashCollisions = weightedExpectedHashCollisions;
    }

    @JsonProperty
    public double getWeightedSumSquaredHashCollisions()
    {
        return weightedSumSquaredHashCollisions;
    }

    @JsonProperty
    public double getWeightedHashCollisions()
    {
        return weightedHashCollisions;
    }

    @JsonProperty
    public double getWeightedExpectedHashCollisions()
    {
        return weightedExpectedHashCollisions;
    }

    @Override
    public HashCollisionsInfo mergeWith(HashCollisionsInfo other)
    {
        return new HashCollisionsInfo(
                this.weightedHashCollisions + other.getWeightedHashCollisions(),
                this.weightedSumSquaredHashCollisions + other.getWeightedSumSquaredHashCollisions(),
                this.weightedExpectedHashCollisions + other.getWeightedExpectedHashCollisions());
    }
}
