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

import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

@Immutable
public class DynamicFilterSummary
        implements Serializable
{
    private final BloomFilterForDynamicFilter bloomFilterForDynamicfilter;

    @JsonCreator
    public DynamicFilterSummary(@JsonProperty("bloomFilterForDynamicFilter") BloomFilterForDynamicFilter bloomFilterForDynamicfilter)
    {
        this.bloomFilterForDynamicfilter = bloomFilterForDynamicfilter;
    }

    @JsonProperty
    public BloomFilterForDynamicFilter getBloomFilterForDynamicFilter()
    {
        return bloomFilterForDynamicfilter;
    }

    public DynamicFilterSummary mergeWith(DynamicFilterSummary other)
    {
        return new DynamicFilterSummary(bloomFilterForDynamicfilter.merge(other.getBloomFilterForDynamicFilter()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DynamicFilterSummary summary = (DynamicFilterSummary) o;

        if (bloomFilterForDynamicfilter != null) {
            return bloomFilterForDynamicfilter.equals(summary.getBloomFilterForDynamicFilter());
        }
        else {
            return summary.getBloomFilterForDynamicFilter() == null;
        }
    }

    @Override
    public int hashCode()
    {
        return bloomFilterForDynamicfilter != null ? bloomFilterForDynamicfilter.hashCode() : 0;
    }

    public void writeObject(ObjectOutputStream out)
            throws IOException
    {
        bloomFilterForDynamicfilter.writeObject(out);
    }

    public void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException
    {
        bloomFilterForDynamicfilter.readObject(in);
    }
}
