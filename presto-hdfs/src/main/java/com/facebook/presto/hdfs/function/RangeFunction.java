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
package com.facebook.presto.hdfs.function;

import com.facebook.presto.hdfs.exception.UnSupportedFunctionException;

/**
 * presto-root
 *
 * @author Jelly
 */
public abstract class RangeFunction implements Function
{
    private final long start;
    private final long stride;
    private final long end;

    public RangeFunction(long start, long stride, long end)
    {
        this.start = start;
        this.stride = stride;
        this.end = end;
    }

    @Override
    public long apply(String v)
    {
        throw new UnSupportedFunctionException("Unsupported range function on string value");
    }

    @Override
    public abstract long apply(int v);

    @Override
    public abstract long apply(long v);

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();
}
