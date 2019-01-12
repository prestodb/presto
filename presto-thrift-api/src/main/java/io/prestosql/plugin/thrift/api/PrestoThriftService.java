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
package io.prestosql.plugin.thrift.api;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TException;
import io.airlift.drift.annotations.ThriftException;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;

import java.util.List;

/**
 * Presto Thrift service definition.
 * This thrift service needs to be implemented in order to be used with Thrift Connector.
 */
@ThriftService(value = "presto", idlName = "PrestoThriftService")
public interface PrestoThriftService
{
    /**
     * Returns available schema names.
     */
    @ThriftMethod("prestoListSchemaNames")
    List<String> listSchemaNames()
            throws PrestoThriftServiceException, TException;

    /**
     * Returns tables for the given schema name.
     *
     * @param schemaNameOrNull a structure containing schema name or {@literal null}
     * @return a list of table names with corresponding schemas. If schema name is null then returns
     * a list of tables for all schemas. Returns an empty list if a schema does not exist
     */
    @ThriftMethod("prestoListTables")
    List<PrestoThriftSchemaTableName> listTables(
            @ThriftField(name = "schemaNameOrNull") PrestoThriftNullableSchemaName schemaNameOrNull)
            throws PrestoThriftServiceException, TException;

    /**
     * Returns metadata for a given table.
     *
     * @param schemaTableName schema and table name
     * @return metadata for a given table, or a {@literal null} value inside if it does not exist
     */
    @ThriftMethod("prestoGetTableMetadata")
    PrestoThriftNullableTableMetadata getTableMetadata(
            @ThriftField(name = "schemaTableName") PrestoThriftSchemaTableName schemaTableName)
            throws PrestoThriftServiceException, TException;

    /**
     * Returns a batch of splits.
     *
     * @param schemaTableName schema and table name
     * @param desiredColumns a superset of columns to return; empty set means "no columns", {@literal null} set means "all columns"
     * @param outputConstraint constraint on the returned data
     * @param maxSplitCount maximum number of splits to return
     * @param nextToken token from a previous split batch or {@literal null} if it is the first call
     * @return a batch of splits
     */
    @ThriftMethod(value = "prestoGetSplits",
            exception = @ThriftException(type = PrestoThriftServiceException.class, id = 1))
    ListenableFuture<PrestoThriftSplitBatch> getSplits(
            @ThriftField(name = "schemaTableName") PrestoThriftSchemaTableName schemaTableName,
            @ThriftField(name = "desiredColumns") PrestoThriftNullableColumnSet desiredColumns,
            @ThriftField(name = "outputConstraint") PrestoThriftTupleDomain outputConstraint,
            @ThriftField(name = "maxSplitCount") int maxSplitCount,
            @ThriftField(name = "nextToken") PrestoThriftNullableToken nextToken);

    /**
     * Returns a batch of index splits for the given batch of keys.
     * This method is called if index join strategy is chosen for a query.
     *
     * @param schemaTableName schema and table name
     * @param indexColumnNames specifies columns and their order for keys
     * @param outputColumnNames a list of column names to return
     * @param keys keys for which records need to be returned; includes only unique and non-null values
     * @param outputConstraint constraint on the returned data
     * @param maxSplitCount maximum number of splits to return
     * @param nextToken token from a previous split batch or {@literal null} if it is the first call
     * @return a batch of splits
     */
    @ThriftMethod(value = "prestoGetIndexSplits",
            exception = @ThriftException(type = PrestoThriftServiceException.class, id = 1))
    ListenableFuture<PrestoThriftSplitBatch> getIndexSplits(
            @ThriftField(name = "schemaTableName") PrestoThriftSchemaTableName schemaTableName,
            @ThriftField(name = "indexColumnNames") List<String> indexColumnNames,
            @ThriftField(name = "outputColumnNames") List<String> outputColumnNames,
            @ThriftField(name = "keys") PrestoThriftPageResult keys,
            @ThriftField(name = "outputConstraint") PrestoThriftTupleDomain outputConstraint,
            @ThriftField(name = "maxSplitCount") int maxSplitCount,
            @ThriftField(name = "nextToken") PrestoThriftNullableToken nextToken);

    /**
     * Returns a batch of rows for the given split.
     *
     * @param splitId split id as returned in split batch
     * @param columns a list of column names to return
     * @param maxBytes maximum size of returned data in bytes
     * @param nextToken token from a previous batch or {@literal null} if it is the first call
     * @return a batch of table data
     */
    @ThriftMethod(value = "prestoGetRows",
            exception = @ThriftException(type = PrestoThriftServiceException.class, id = 1))
    ListenableFuture<PrestoThriftPageResult> getRows(
            @ThriftField(name = "splitId") PrestoThriftId splitId,
            @ThriftField(name = "columns") List<String> columns,
            @ThriftField(name = "maxBytes") long maxBytes,
            @ThriftField(name = "nextToken") PrestoThriftNullableToken nextToken);
}
