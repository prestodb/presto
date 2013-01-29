package com.facebook.presto.execution;

import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;

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
