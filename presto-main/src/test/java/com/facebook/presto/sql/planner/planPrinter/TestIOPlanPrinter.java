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

package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.Session;
import com.facebook.presto.common.WarningHandlingLevel;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.execution.warnings.DefaultWarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.metadata.AbstractMockMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardWarningCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PRINT_WARNINGS_FOR_EXPLAIN_IO;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestIOPlanPrinter
{
    private static final Session WARNINGS_DISABLED_SESSION = testSessionBuilder()
            .setSystemProperty(PRINT_WARNINGS_FOR_EXPLAIN_IO, "false")
            .build();
    private static final Session WARNINGS_ENABLED_SESSION = testSessionBuilder()
            .setSystemProperty(PRINT_WARNINGS_FOR_EXPLAIN_IO, "true")
            .build();

    private static final Metadata METADATA = new TestingMetadataManager();

    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(WARNINGS_ENABLED_SESSION, new PlanNodeIdAllocator(), METADATA);
    private static final PlanNode TEST_PLAN = PLAN_BUILDER.tableFinish(
            new TableWriterNode.CreateName(
                    new ConnectorId("testConnector"),
                    new ConnectorTableMetadata(new SchemaTableName("test_schema", "test_table"), ImmutableList.of()),
                    Optional.empty()),
            PLAN_BUILDER.tableScan("testConnector",
                    ImmutableList.of(PLAN_BUILDER.variable("c1")),
                    ImmutableMap.of(PLAN_BUILDER.variable("c1"), new TestingColumnHandle("column1"))));

    @Test
    public void testIOPlanWithWarningsDisabled()
    {
        WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig(), WarningHandlingLevel.NORMAL);
        warningCollector.add(new PrestoWarning(StandardWarningCode.PARSER_WARNING, "Warning about something"));
        String actualIOPlan = IOPlanPrinter.textIOPlan(TEST_PLAN, METADATA, WARNINGS_DISABLED_SESSION, warningCollector);
        String expectedIOPlan = "{\n" +
                "  \"inputTableColumnInfos\" : [ {\n" +
                "    \"table\" : {\n" +
                "      \"catalog\" : \"testConnector\",\n" +
                "      \"schemaTable\" : {\n" +
                "        \"schema\" : \"test_schema\",\n" +
                "        \"table\" : \"test_table\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"columnConstraints\" : [ ]\n" +
                "  } ],\n" +
                "  \"outputTable\" : {\n" +
                "    \"catalog\" : \"testConnector\",\n" +
                "    \"schemaTable\" : {\n" +
                "      \"schema\" : \"test_schema\",\n" +
                "      \"table\" : \"test_table\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        assertEquals(actualIOPlan, expectedIOPlan);
    }

    @Test
    public void testIOPlanWithWarningsEnabled()
    {
        WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig(), WarningHandlingLevel.NORMAL);
        warningCollector.add(new PrestoWarning(StandardWarningCode.PARSER_WARNING, "Warning about something"));
        PlanBuilder planBuilder = new PlanBuilder(WARNINGS_ENABLED_SESSION, new PlanNodeIdAllocator(), METADATA);
        String actualIOPlan = IOPlanPrinter.textIOPlan(TEST_PLAN, METADATA, WARNINGS_ENABLED_SESSION, warningCollector);
        String expectedIOPlan = "{\n" +
                "  \"inputTableColumnInfos\" : [ {\n" +
                "    \"table\" : {\n" +
                "      \"catalog\" : \"testConnector\",\n" +
                "      \"schemaTable\" : {\n" +
                "        \"schema\" : \"test_schema\",\n" +
                "        \"table\" : \"test_table\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"columnConstraints\" : [ ]\n" +
                "  } ],\n" +
                "  \"outputTable\" : {\n" +
                "    \"catalog\" : \"testConnector\",\n" +
                "    \"schemaTable\" : {\n" +
                "      \"schema\" : \"test_schema\",\n" +
                "      \"table\" : \"test_table\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"warnings\" : [ {\n" +
                "    \"warningCode\" : {\n" +
                "      \"code\" : 2,\n" +
                "      \"name\" : \"PARSER_WARNING\"\n" +
                "    },\n" +
                "    \"message\" : \"Warning about something\"\n" +
                "  } ]\n" +
                "}";
        assertEquals(actualIOPlan, expectedIOPlan);
    }

    @Test
    public void testIOPlanWithEmptyWarnings()
    {
        WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig(), WarningHandlingLevel.NORMAL);
        String actualIOPlan = IOPlanPrinter.textIOPlan(TEST_PLAN, METADATA, WARNINGS_DISABLED_SESSION, warningCollector);
        String expectedIOPlan = "{\n" +
                "  \"inputTableColumnInfos\" : [ {\n" +
                "    \"table\" : {\n" +
                "      \"catalog\" : \"testConnector\",\n" +
                "      \"schemaTable\" : {\n" +
                "        \"schema\" : \"test_schema\",\n" +
                "        \"table\" : \"test_table\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"columnConstraints\" : [ ]\n" +
                "  } ],\n" +
                "  \"outputTable\" : {\n" +
                "    \"catalog\" : \"testConnector\",\n" +
                "    \"schemaTable\" : {\n" +
                "      \"schema\" : \"test_schema\",\n" +
                "      \"table\" : \"test_table\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        assertEquals(actualIOPlan, expectedIOPlan);
    }

    private static class TestingMetadataManager
            extends AbstractMockMetadata
    {
        @Override
        public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
        {
            return new TableMetadata(
                    tableHandle.getConnectorId(),
                    new ConnectorTableMetadata(
                            new SchemaTableName("test_schema", "test_table"),
                            ImmutableList.of(new ColumnMetadata("column1", BIGINT))));
        }

        @Override
        public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return new ColumnMetadata("column1", BIGINT);
        }

        @Override
        public TupleDomain<ColumnHandle> toExplainIOConstraints(Session session, TableHandle tableHandle, TupleDomain<ColumnHandle> constraints)
        {
            return constraints;
        }
    }
}
