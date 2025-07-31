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

import com.facebook.presto.execution.Output;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.google.common.base.VerifyException;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.connector.ConnectorCommitHandle.EMPTY_COMMIT_OUTPUT;
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
                visitor.getSchemaTableName().getTableName(),
                EMPTY_COMMIT_OUTPUT,
                visitor.getOutputColumns()));
    }

    private class Visitor
            extends InternalPlanVisitor<Void, Void>
    {
        private ConnectorId connectorId;
        private SchemaTableName schemaTableName;
        private Optional<List<OutputColumnMetadata>> outputColumns = Optional.empty();

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            TableWriterNode.WriterTarget writerTarget = node.getTarget().orElseThrow(() -> new VerifyException("target is absent"));
            connectorId = writerTarget.getConnectorId();
            checkState(schemaTableName == null || schemaTableName.equals(writerTarget.getSchemaTableName()),
                    "cannot have more than a single create, insert or delete in a query");
            schemaTableName = writerTarget.getSchemaTableName();
            outputColumns = writerTarget.getOutputColumns();
            return null;
        }

        public Void visitSequence(SequenceNode node, Void context)
        {
            // Left children of sequence are ignored since they don't output anything
            node.getPrimarySource().accept(this, context);

            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
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

        public Optional<List<OutputColumnMetadata>> getOutputColumns()
        {
            return outputColumns;
        }
    }
}
