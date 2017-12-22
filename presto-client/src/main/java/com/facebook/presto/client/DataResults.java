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
package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.facebook.presto.client.FixJsonDataUtils.fixData;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Iterables.unmodifiableIterable;

@Immutable
public class DataResults
        implements QueryData
{
    private final Iterable<List<Object>> data;

    @JsonCreator
    public DataResults(@JsonProperty("data") Iterable<List<Object>> data)
    {
        this.data = (data == null) ? null : unmodifiableIterable(data);
    }

    @Nullable
    @JsonProperty
    @Override
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hasData", data != null)
                .toString();
    }

    public DataResults withFixedData(List<Column> columns)
    {
        if (data == null) {
            // nothing to fix
            return this;
        }
        return new DataResults(fixData(columns, data));
    }
}
