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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.connector.thrift.api.PrestoThriftId;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableColumnSet;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftPageResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.facebook.presto.connector.thrift.api.PrestoThriftSession;
import com.facebook.presto.connector.thrift.api.PrestoThriftSessionProperty;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TestingThriftService
        implements PrestoThriftService
{
    private final List<PrestoThriftSessionProperty> sessionProperties;

    public TestingThriftService()
    {
        this(ImmutableList.of());
    }

    public TestingThriftService(List<PropertyMetadata<?>> sessionProperties)
    {
        requireNonNull(sessionProperties, "sessionProperties is null");
        this.sessionProperties = sessionProperties.stream()
                .map(PrestoThriftSessionProperty::fromProperty)
                .collect(toImmutableList());
    }

    @Override
    public List<String> listSchemaNames()
            throws PrestoThriftServiceException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public List<PrestoThriftSessionProperty> listSessionProperties()
            throws PrestoThriftServiceException
    {
        return sessionProperties;
    }

    @Override
    public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
            throws PrestoThriftServiceException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
            throws PrestoThriftServiceException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ListenableFuture<PrestoThriftSplitBatch> getSplits(PrestoThriftSchemaTableName schemaTableName, PrestoThriftNullableColumnSet desiredColumns, PrestoThriftTupleDomain outputConstraint, int maxSplitCount, PrestoThriftNullableToken nextToken, PrestoThriftSession session)
            throws PrestoThriftServiceException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ListenableFuture<PrestoThriftSplitBatch> getIndexSplits(PrestoThriftSchemaTableName schemaTableName, List<String> indexColumnNames, List<String> outputColumnNames, PrestoThriftPageResult keys, PrestoThriftTupleDomain outputConstraint, int maxSplitCount, PrestoThriftNullableToken nextToken, PrestoThriftSession session)
            throws PrestoThriftServiceException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public ListenableFuture<PrestoThriftPageResult> getRows(PrestoThriftId splitId, List<String> columns, long maxBytes, PrestoThriftNullableToken nextToken, PrestoThriftSession session)
            throws PrestoThriftServiceException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void close()
    {
    }
}
