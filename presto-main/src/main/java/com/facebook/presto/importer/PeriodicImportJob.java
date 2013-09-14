/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.importer;

import com.facebook.presto.metadata.QualifiedTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;

public final class PeriodicImportJob
{
    private final QualifiedTableName srcTable;
    private final QualifiedTableName dstTable;
    private final long intervalSeconds;

    public static PeriodicImportJob createJob(QualifiedTableName srcTable, QualifiedTableName dstTable, long intervalSeconds)
    {
        return new PeriodicImportJob(srcTable, dstTable, intervalSeconds);
    }

    @JsonCreator
    public PeriodicImportJob(@JsonProperty("srcCatalogName") String srcCatalogName,
            @JsonProperty("srcSchemaName") String srcSchemaName,
            @JsonProperty("srcTableName") String srcTableName,
            @JsonProperty("dstCatalogName") String dstCatalogName,
            @JsonProperty("dstSchemaName") String dstSchemaName,
            @JsonProperty("dstTableName") String dstTableName,
            @JsonProperty("intervalSeconds") long intervalSeconds)
    {
        this(new QualifiedTableName(srcCatalogName, srcSchemaName, srcTableName),
                new QualifiedTableName(dstCatalogName, dstSchemaName, dstTableName),
                intervalSeconds);
    }

    PeriodicImportJob(QualifiedTableName srcTable,
            QualifiedTableName dstTable,
            long intervalSeconds)
    {
        this.srcTable = checkTable(srcTable);
        this.dstTable = checkTable(dstTable);

        this.intervalSeconds = intervalSeconds;
    }

    @JsonProperty
    public String getSrcCatalogName()
    {
        return srcTable.getCatalogName();
    }

    @JsonProperty
    public String getSrcSchemaName()
    {
        return srcTable.getSchemaName();
    }

    @JsonProperty
    public String getSrcTableName()
    {
        return srcTable.getTableName();
    }

    @JsonProperty
    public String getDstCatalogName()
    {
        return dstTable.getCatalogName();
    }

    @JsonProperty
    public String getDstSchemaName()
    {
        return dstTable.getSchemaName();
    }

    @JsonProperty
    public String getDstTableName()
    {
        return dstTable.getTableName();
    }

    @JsonProperty
    public long getIntervalSeconds()
    {
        return intervalSeconds;
    }

    public QualifiedTableName getSrcTable()
    {
        return srcTable;
    }

    public QualifiedTableName getDstTable()
    {
        return dstTable;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("srcTable", srcTable)
                .add("dstTable", dstTable)
                .add("intervalSeconds", intervalSeconds)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(srcTable, dstTable, intervalSeconds);
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
        return Objects.equal(srcTable, other.srcTable)
                && Objects.equal(dstTable, other.dstTable)
                && intervalSeconds == other.intervalSeconds;
    }
}
