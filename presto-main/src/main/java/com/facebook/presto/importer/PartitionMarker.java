package com.facebook.presto.importer;

import com.google.common.base.Preconditions;

public class PartitionMarker
{
    private final long tableId;
    private final String partitionName;

    public PartitionMarker(long tableId, String partitionName)
    {
        this.tableId = tableId;
        this.partitionName = Preconditions.checkNotNull(partitionName, "partitionName is null");
    }

    public static PartitionMarker from(long tableId, String partitionName)
    {
        return new PartitionMarker(tableId, partitionName);
    }

    public long getTableId()
    {
        return tableId;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionMarker that = (PartitionMarker) o;

        if (tableId != that.tableId) {
            return false;
        }
        if (!partitionName.equals(that.partitionName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (tableId ^ (tableId >>> 32));
        result = 31 * result + partitionName.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "PartitionMarker{" +
                "tableId=" + tableId +
                ", partitionName='" + partitionName + '\'' +
                '}';
    }
}
