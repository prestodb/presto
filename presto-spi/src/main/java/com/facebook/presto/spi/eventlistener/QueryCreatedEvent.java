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

import java.util.Date;

public class QueryCreatedEvent
{
    private final Date createTime;

    private final QueryContext queryContext;
    private final QueryMetadata queryMetadata;

    public QueryCreatedEvent(Date createTime, QueryContext queryContext, QueryMetadata queryMetadata)
    {
        this.createTime = createTime;
        this.queryContext = queryContext;
        this.queryMetadata = queryMetadata;
    }

    public Date getCreateTime()
    {
        return createTime;
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
