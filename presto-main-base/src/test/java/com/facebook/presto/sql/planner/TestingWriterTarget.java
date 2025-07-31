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

package com.facebook.presto.sql.planner;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.SourceColumn;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

public class TestingWriterTarget
        extends TableWriterNode.WriterTarget
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("test");
    private static final SchemaTableName SCHEMA_TABLE_NAME = new SchemaTableName("test-schema", "test-table");

    @Override
    public ConnectorId getConnectorId()
    {
        return CONNECTOR_ID;
    }

    @Override
    public SchemaTableName getSchemaTableName()
    {
        return SCHEMA_TABLE_NAME;
    }

    @Override
    public Optional<List<OutputColumnMetadata>> getOutputColumns()
    {
        return Optional.of(
                ImmutableList.of(
                        new OutputColumnMetadata(
                                "column", "type",
                                ImmutableSet.of(
                                        new SourceColumn(QualifiedObjectName.valueOf("catalog.schema.table"), "column")))));
    }

    @Override
    public String toString()
    {
        return "testing handle";
    }
}
