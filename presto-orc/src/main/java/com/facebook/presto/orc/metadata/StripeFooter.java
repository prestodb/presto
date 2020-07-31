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
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class StripeFooter
{
    private final List<Stream> streams;
    private final Map<Integer, ColumnEncoding> columnEncodings;

    // encrypted StripeEncryptionGroups
    private final List<Slice> stripeEncryptionGroups;

    public StripeFooter(List<Stream> streams, Map<Integer, ColumnEncoding> columnEncodings, List<Slice> stripeEncryptionGroups)
    {
        this.streams = ImmutableList.copyOf(requireNonNull(streams, "streams is null"));
        this.columnEncodings = ImmutableMap.copyOf(requireNonNull(columnEncodings, "columnEncodings is null"));
        this.stripeEncryptionGroups = ImmutableList.copyOf(requireNonNull(stripeEncryptionGroups, "stripeEncryptionGroups is null"));
    }

    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        return columnEncodings;
    }

    public List<Stream> getStreams()
    {
        return streams;
    }

    public List<Slice> getStripeEncryptionGroups()
    {
        return stripeEncryptionGroups;
    }
}
