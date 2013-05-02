package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class QueryError
{
    private final String message;
    private final String sqlState;
    private final int errorCode;
    private final ErrorLocation errorLocation;
    private final FailureInfo failureInfo;

    @JsonCreator
    public QueryError(
            @JsonProperty("message") String message,
            @JsonProperty("sqlState") String sqlState,
            @JsonProperty("errorCode") int errorCode,
            @JsonProperty("errorLocation") ErrorLocation errorLocation,
            @JsonProperty("failureInfo") FailureInfo failureInfo)
    {
        this.message = message;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
        this.errorLocation = errorLocation;
        this.failureInfo = failureInfo;
    }

    @NotNull
    @JsonProperty
    public String getMessage()
    {
        return message;
    }

    @Nullable
    @JsonProperty
    public String getSqlState()
    {
        return sqlState;
    }

    @JsonProperty
    public int getErrorCode()
    {
        return errorCode;
    }

    @Nullable
    @JsonProperty
    public ErrorLocation getErrorLocation()
    {
        return errorLocation;
    }

    @Nullable
    @JsonProperty
    public FailureInfo getFailureInfo()
    {
        return failureInfo;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("message", message)
                .add("sqlState", sqlState)
                .add("errorCode", errorCode)
                .add("errorLocation", errorLocation)
                .add("failureInfo", failureInfo)
                .toString();
    }
}
