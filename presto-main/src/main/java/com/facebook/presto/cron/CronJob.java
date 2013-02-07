package com.facebook.presto.cron;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class CronJob
{
    private final String srcCatalogName;
    private final String srcSchemaName;
    private final String srcTableName;
    private final String dstCatalogName;
    private final String dstSchemaName;
    private final String dstTableName;
    private final long interval;

    @JsonCreator
    public CronJob(@JsonProperty("srcCatalogName") String srcCatalogName,
            @JsonProperty("srcSchemaName") String srcSchemaName,
            @JsonProperty("srcTableName") String srcTableName,
            @JsonProperty("dstCatalogName") String dstCatalogName,
            @JsonProperty("dstSchemaName") String dstSchemaName,
            @JsonProperty("dstTableName") String dstTableName,
            @JsonProperty("interval") long interval)
    {
        this.srcCatalogName = srcCatalogName;
        this.srcSchemaName = srcSchemaName;
        this.srcTableName = srcTableName;

        this.dstCatalogName = dstCatalogName;
        this.dstSchemaName = dstSchemaName;
        this.dstTableName = dstTableName;

        this.interval = interval;
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
    public long getInterval()
    {
        return interval;
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
                .add("interval", interval)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(srcCatalogName, srcSchemaName, srcTableName,
                dstCatalogName, dstSchemaName, dstTableName,
                interval);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        else if (obj == null) {
            return false;
        }
        else if (getClass() != obj.getClass()) {
            return false;
        }
        CronJob other = (CronJob) obj;
        return Objects.equal(srcCatalogName, other.srcCatalogName)
                && Objects.equal(srcSchemaName, other.srcSchemaName)
                && Objects.equal(srcTableName, other.srcTableName)
                && Objects.equal(dstCatalogName, other.dstCatalogName)
                && Objects.equal(dstSchemaName, other.dstSchemaName)
                && Objects.equal(dstTableName, other.dstTableName)
                && Objects.equal(interval, other.interval);
    }
}