package com.facebook.presto.importer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public final class PeriodicImportJob
{
    private final String srcCatalogName;
    private final String srcSchemaName;
    private final String srcTableName;
    private final String dstCatalogName;
    private final String dstSchemaName;
    private final String dstTableName;
    private final long intervalSeconds;

    @JsonCreator
    public PeriodicImportJob(@JsonProperty("srcCatalogName") String srcCatalogName,
            @JsonProperty("srcSchemaName") String srcSchemaName,
            @JsonProperty("srcTableName") String srcTableName,
            @JsonProperty("dstCatalogName") String dstCatalogName,
            @JsonProperty("dstSchemaName") String dstSchemaName,
            @JsonProperty("dstTableName") String dstTableName,
            @JsonProperty("intervalSeconds") long intervalSeconds)
    {
        this.srcCatalogName = srcCatalogName;
        this.srcSchemaName = srcSchemaName;
        this.srcTableName = srcTableName;

        this.dstCatalogName = dstCatalogName;
        this.dstSchemaName = dstSchemaName;
        this.dstTableName = dstTableName;

        this.intervalSeconds = intervalSeconds;
    }

    @JsonProperty
    public String getSrcCatalogName()
    {
        return srcCatalogName;
    }

    @JsonProperty
    public String getSrcSchemaName()
    {
        return srcSchemaName;
    }

    @JsonProperty
    public String getSrcTableName()
    {
        return srcTableName;
    }

    @JsonProperty
    public String getDstCatalogName()
    {
        return dstCatalogName;
    }

    @JsonProperty
    public String getDstSchemaName()
    {
        return dstSchemaName;
    }

    @JsonProperty
    public String getDstTableName()
    {
        return dstTableName;
    }

    @JsonProperty
    public long getIntervalSeconds()
    {
        return intervalSeconds;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("srcCatalogName", srcCatalogName)
                .add("srcSchemaName", srcSchemaName)
                .add("srcTableName", srcTableName)
                .add("dstCatalogName", dstCatalogName)
                .add("dstSchemaName", dstSchemaName)
                .add("dstTableName", dstTableName)
                .add("intervalSeconds", intervalSeconds)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(srcCatalogName, srcSchemaName, srcTableName,
                dstCatalogName, dstSchemaName, dstTableName,
                intervalSeconds);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        else if ((obj == null) || getClass() != obj.getClass()) {
            return false;
        }

        PeriodicImportJob other = (PeriodicImportJob) obj;
        return Objects.equal(srcCatalogName, other.srcCatalogName)
                && Objects.equal(srcSchemaName, other.srcSchemaName)
                && Objects.equal(srcTableName, other.srcTableName)
                && Objects.equal(dstCatalogName, other.dstCatalogName)
                && Objects.equal(dstSchemaName, other.dstSchemaName)
                && Objects.equal(dstTableName, other.dstTableName)
                && intervalSeconds == other.intervalSeconds;
    }
}
