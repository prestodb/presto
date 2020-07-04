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
package com.facebook.presto.orc.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class StripeEncryptionGroup
        implements Serializable
{
    private final List<Stream> streams;
    private final Map<Integer, ColumnEncoding> columnEncodings;

    public StripeEncryptionGroup(List<Stream> streams, Map<Integer, ColumnEncoding> columnEncodings)
    {
        this.streams = requireNonNull(ImmutableList.copyOf(streams), "streams is null");
        this.columnEncodings = requireNonNull(ImmutableMap.copyOf(columnEncodings), "columnEncodings is null");
    }

    public List<Stream> getStreams()
    {
        return streams;
    }

    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        return columnEncodings;
    }
}
