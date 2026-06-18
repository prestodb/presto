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

import static java.util.Objects.requireNonNull;

public class QueryUpdatedEvent
{
    private final QueryMetadata metadata;

    public QueryUpdatedEvent(QueryMetadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public QueryMetadata getMetadata()
    {
        return metadata;
    }
}
