package com.facebook.presto.tpch;

import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.split.PartitionedSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

// Right now, splits are just the entire TPCH table
public class TpchSplit
        implements PartitionedSplit
{
    private final TpchTableHandle tableHandle;
    private final int tableSplit;
    private final int tableSkew;

    private final String partition;

    @JsonCreator
    public TpchSplit(@JsonProperty("tableHandle") TpchTableHandle tableHandle,
            @JsonProperty("tableSkew") int tableSkew,
            @JsonProperty("tableSplit") int tableSplit)
    {
        this.tableHandle = checkNotNull(tableHandle, "tableHandle is null");
        this.tableSkew = tableSkew;
        this.tableSplit = tableSplit;
        this.partition = "tpch_split_" + tableSkew;
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
    public int getTableSplit()
    {
        return tableSplit;
    }

    @JsonProperty
    public int getTableSkew()
    {
        return tableSkew;
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
                && tableSkew == tpchSplit.tableSkew
                && tableSplit == tpchSplit.tableSplit) {
            return true;
        }

        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableHandle, tableSkew, tableSplit);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tableHandle", tableHandle)
                .add("tableSkew", tableSkew)
                .add("tableSplit", tableSplit)
                .toString();
    }
}
