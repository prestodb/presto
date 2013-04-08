package com.facebook.presto.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class ErrorLocation
{
    private final int lineNumber;
    private final int columnNumber;

    @JsonCreator
    public ErrorLocation(
            @JsonProperty("lineNumber") int lineNumber,
            @JsonProperty("columnNumber") int columnNumber)
    {
        checkArgument(lineNumber >= 1, "lineNumber must be at least one");
        checkArgument(columnNumber >= 1, "columnNumber must be at least one");

        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
    }

    @JsonProperty
    public int getLineNumber()
    {
        return lineNumber;
    }

    @JsonProperty
    public int getColumnNumber()
    {
        return columnNumber;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("lineNumber", lineNumber)
                .add("columnNumber", columnNumber)
                .toString();
    }
}
