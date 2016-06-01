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

package com.facebook.presto.spi.eventlistener;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class QueryCreatedEvent
{
    private final Instant createTime;

    private final QueryContext context;
    private final QueryMetadata metadata;

    public QueryCreatedEvent(Instant createTime, QueryContext context, QueryMetadata metadata)
    {
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.context = requireNonNull(context, "context is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public Instant getCreateTime()
    {
        return createTime;
    }

    public QueryContext getContext()
    {
        return context;
    }

    public QueryMetadata getMetadata()
    {
        return metadata;
    }
}
