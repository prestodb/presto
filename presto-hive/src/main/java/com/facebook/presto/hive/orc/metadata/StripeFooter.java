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
package com.facebook.presto.hive.orc.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class StripeFooter
{
    private final List<Stream> streams;
    private final List<ColumnEncoding> columnEncodings;

    public StripeFooter(List<Stream> streams, List<ColumnEncoding> columnEncodings)
    {
        this.streams = ImmutableList.copyOf(checkNotNull(streams, "streams is null"));
        this.columnEncodings = ImmutableList.copyOf(checkNotNull(columnEncodings, "columnEncodings is null"));
    }

    public List<ColumnEncoding> getColumnEncodings()
    {
        return columnEncodings;
    }

    public List<Stream> getStreams()
    {
        return streams;
    }
}
