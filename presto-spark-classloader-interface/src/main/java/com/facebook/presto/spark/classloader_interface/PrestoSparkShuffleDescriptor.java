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
package com.facebook.presto.spark.classloader_interface;

import org.apache.spark.shuffle.ShuffleHandle;

/**
 * PrestoSparkShuffleDescriptor is used as a container to carry over Spark shuffle related meta information
 * (e.g. ShuffleHandle, number of partitions etc.) from Spark related structures (Spark Rdd, Spark Partition etc.)
 * to Presto-Spark land. These meta information are essential to delegate the shuffle operation from JVM to external
 * native execution process.
 */
public abstract class PrestoSparkShuffleDescriptor
{
    private final ShuffleHandle shuffleHandle;
    private final int numberOfPartition;

    public PrestoSparkShuffleDescriptor(ShuffleHandle shuffleHandle, int numberOfPartition)
    {
        this.shuffleHandle = requireNonNull(shuffleHandle, "shuffleHandle is null");
        checkArgument(numberOfPartition > 0, "numberOfPartition requires larger than 0");
        this.numberOfPartition = numberOfPartition;
    }

    public int getNumberOfPartition()
    {
        return numberOfPartition;
    }

    public ShuffleHandle getShuffleHandle()
    {
        return shuffleHandle;
    }

    private <T> T requireNonNull(T object, String message)
    {
        if (object == null) {
            throw new RuntimeException(message);
        }

        return object;
    }

    private void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new RuntimeException(message);
        }
    }
}
