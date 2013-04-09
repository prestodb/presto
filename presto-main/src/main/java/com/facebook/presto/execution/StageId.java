/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;

import java.util.List;

import static com.facebook.presto.execution.QueryId.validateId;
import static com.google.common.base.Preconditions.checkNotNull;

public class StageId
{
    @JsonCreator
    public static StageId valueOf(String stageId)
    {
        List<String> ids = QueryId.parseDottedId(stageId, 2, "stageId");
        return new StageId(new QueryId(ids.get(0)), ids.get(1));
    }

    private final QueryId queryId;
    private final String id;

    public StageId(QueryId queryId, String id)
    {
        this.queryId = checkNotNull(queryId, "queryId is null");
        this.id = validateId(id);
    }

    public StageId(String queryId, String id)
    {
        this.queryId = new QueryId(queryId);
        this.id = validateId(id);
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public String getId()
    {
        return id;
    }

    @JsonValue
    public String toString()
    {
        return queryId + "." + id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id, queryId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final StageId other = (StageId) obj;
        return Objects.equal(this.id, other.id) &&
                Objects.equal(this.queryId, other.queryId);
    }
}
