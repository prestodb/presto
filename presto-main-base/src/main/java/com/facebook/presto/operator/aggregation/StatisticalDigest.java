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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public interface StatisticalDigest<T>
{
    default void add(double value, long weight)
    {
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "cannot add a double to a q-digest");
    }

    default void add(long value, long weight)
    {
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "cannot add a long to a t-digest");
    }

    void merge(StatisticalDigest other);

    long estimatedInMemorySizeInBytes();

    Slice serialize();

    StatisticalDigest<T> getDigest();

    double getSize();
}
