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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.Output;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.TableWriterNode;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class OutputExtractor
{
    public Optional<Output> extractOutput(PlanNode root)
    {
        Visitor visitor = new Visitor();
        root.accept(visitor, null);

        if (visitor.getConnectorId() == null) {
            return Optional.empty();
        }

        return Optional.of(new Output(
                visitor.getConnectorId(),
                visitor.getSchemaTableName().getSchemaName(),
                visitor.getSchemaTableName().getTableName()));
    }

    private class Visitor
            extends PlanVisitor<Void, Void>
    {
        private ConnectorId connectorId = null;
        private SchemaTableName schemaTableName = null;

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            TableWriterNode.WriterTarget writerTarget = node.getTarget();

            if (writerTarget instanceof TableWriterNode.CreateHandle) {
                connectorId = ((TableWriterNode.CreateHandle) writerTarget).getHandle().getConnectorId();
                checkState(schemaTableName == null || schemaTableName.equals(((TableWriterNode.CreateHandle) writerTarget).getSchemaTableName()),
                        "cannot have more than a single create, insert or delete in a query");
                schemaTableName = ((TableWriterNode.CreateHandle) writerTarget).getSchemaTableName();
            }
            if (writerTarget instanceof TableWriterNode.InsertHandle) {
                connectorId = ((TableWriterNode.InsertHandle) writerTarget).getHandle().getConnectorId();
                checkState(schemaTableName == null || schemaTableName.equals(((TableWriterNode.InsertHandle) writerTarget).getSchemaTableName()),
                        "cannot have more than a single create, insert or delete in a query");
                schemaTableName = ((TableWriterNode.InsertHandle) writerTarget).getSchemaTableName();
            }
            if (writerTarget instanceof TableWriterNode.DeleteHandle) {
                connectorId = ((TableWriterNode.DeleteHandle) writerTarget).getHandle().getConnectorId();
                checkState(schemaTableName == null || schemaTableName.equals(((TableWriterNode.DeleteHandle) writerTarget).getSchemaTableName()),
                        "cannot have more than a single create, insert or delete in a query");
                schemaTableName = ((TableWriterNode.DeleteHandle) writerTarget).getSchemaTableName();
            }
            return null;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return null;
        }

        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }
    }
}
