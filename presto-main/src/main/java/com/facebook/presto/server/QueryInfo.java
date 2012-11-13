/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class QueryInfo {
    private final String queryId;
    private final List<TupleInfo> tupleInfos;

    @JsonCreator
    public QueryInfo(@JsonProperty("queryId") String queryId, @JsonProperty("tupleInfos") List<TupleInfo> tupleInfos)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        this.queryId = queryId;
        this.tupleInfos = tupleInfos;
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(queryId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final QueryInfo other = (QueryInfo) obj;
        return Objects.equal(this.queryId, other.queryId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("queryId", queryId)
                .add("tupleInfos", tupleInfos)
                .toString();
    }
}
