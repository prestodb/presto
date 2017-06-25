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

import java.util.function.Supplier;

import static com.facebook.presto.operator.HashCollisionsInfo.createHashCollisionsInfo;

public class HashCollisionsCounter
        implements Supplier<OperatorInfo>
{
    private final OperatorContext operatorContext;

    private long hashCollisions;
    private double expectedHashCollisions;

    public HashCollisionsCounter(OperatorContext operatorContext)
    {
        this.operatorContext = operatorContext;
    }

    public void recordHashCollision(long hashCollisions, double expectedHashCollisions)
    {
        this.hashCollisions += hashCollisions;
        this.expectedHashCollisions += expectedHashCollisions;
    }

    @Override
    public HashCollisionsInfo get()
    {
        return createHashCollisionsInfo(operatorContext.getInputPositions().getTotalCount(), hashCollisions, expectedHashCollisions);
    }
}
