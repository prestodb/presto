package com.facebook.presto.tpch;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.split.PartitionedSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

// Right now, splits are just the entire TPCH table
public class TpchSplit
        implements PartitionedSplit
{
    private final TpchTableHandle tableHandle;
    private final int totalParts;
    private final int partNumber;

    private final String partition;

    @JsonCreator
    public TpchSplit(@JsonProperty("tableHandle") TpchTableHandle tableHandle,
            @JsonProperty("partNumber") int partNumber,
            @JsonProperty("totalParts") int totalParts)
    {
        checkState(partNumber >= 0, "partNumber must be >= 0");
        checkState(totalParts >= 1, "totalParts must be >= 1");
        checkState(totalParts > partNumber, "totalParts must be > partNumber");

        this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        this.partNumber = partNumber;
        this.totalParts = totalParts;
        this.partition = "tpch_part_" + partNumber;
    }

    @VisibleForTesting
    public TpchSplit(TpchTableHandle tableHandle)
    {
        this(tableHandle, 0, 1);
    }

    @Override
    public DataSourceType getDataSourceType()
    {
        return DataSourceType.TPCH;
    }

    @JsonProperty
    public TpchTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public int getTotalParts()
    {
        return totalParts;
    }

    @JsonProperty
    public int getPartNumber()
    {
        return partNumber;
    }

    @Override
    public String getPartition()
    {
        return partition;
    }

    @Override
    public boolean isLastSplit()
    {
        return true;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TpchSplit)) {
            return false;
        }

        TpchSplit tpchSplit = (TpchSplit) o;

        if (tableHandle.equals(tpchSplit.tableHandle)
                && partNumber == tpchSplit.partNumber
                && totalParts == tpchSplit.totalParts) {
            return true;
        }

        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableHandle, partNumber, totalParts);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("partNumber", partNumber)
                .add("totalParts", totalParts)
                .toString();
    }
}
