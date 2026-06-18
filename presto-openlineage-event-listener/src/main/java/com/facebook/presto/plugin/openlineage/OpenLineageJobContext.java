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
package com.facebook.presto.plugin.openlineage;

import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryMetadata;

import static java.util.Objects.requireNonNull;

public class OpenLineageJobContext
{
    private final QueryContext queryContext;
    private final QueryMetadata queryMetadata;

    public OpenLineageJobContext(QueryContext queryContext, QueryMetadata queryMetadata)
    {
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.queryMetadata = requireNonNull(queryMetadata, "queryMetadata is null");
    }

    public QueryContext getQueryContext()
    {
        return queryContext;
    }

    public QueryMetadata getQueryMetadata()
    {
        return queryMetadata;
    }
}
