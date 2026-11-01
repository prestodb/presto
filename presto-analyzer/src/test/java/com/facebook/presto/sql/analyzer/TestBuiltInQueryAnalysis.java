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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.ViewDefinitionReferences;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBuiltInQueryAnalysis
{
    private static final ConnectorId CONNECTOR_A = new ConnectorId("catalog_a");
    private static final ConnectorId CONNECTOR_B = new ConnectorId("catalog_b");
    private static final ConnectorId CONNECTOR_C = new ConnectorId("catalog_c");

    private static final ConnectorTableHandle TABLE_HANDLE = new ConnectorTableHandle() {};
    private static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() {};

    @Test
    public void testExtractConnectorsFromTables()
    {
        Analysis analysis = createAnalysis(null);
        Table table = new Table(QualifiedName.of("t1"));
        analysis.registerTable(table, createTableHandle(CONNECTOR_A));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertEquals(connectors.size(), 1);
        assertTrue(connectors.contains(CONNECTOR_A));
    }

    @Test
    public void testExtractConnectorsFromMultipleTables()
    {
        Analysis analysis = createAnalysis(null);
        analysis.registerTable(new Table(QualifiedName.of("t1")), createTableHandle(CONNECTOR_A));
        analysis.registerTable(new Table(QualifiedName.of("t2")), createTableHandle(CONNECTOR_B));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertEquals(connectors.size(), 2);
        assertTrue(connectors.contains(CONNECTOR_A));
        assertTrue(connectors.contains(CONNECTOR_B));
    }

    @Test
    public void testExtractConnectorsFromInsert()
    {
        Analysis analysis = createAnalysis(null);
        TableHandle insertTarget = createTableHandle(CONNECTOR_B);
        ColumnHandle column = new ColumnHandle() {};
        analysis.setInsert(new Analysis.Insert(insertTarget, ImmutableList.of(column)));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertTrue(connectors.contains(CONNECTOR_B));
    }

    @Test
    public void testExtractConnectorsFromInsertWithSourceTable()
    {
        Analysis analysis = createAnalysis(null);
        analysis.registerTable(new Table(QualifiedName.of("source")), createTableHandle(CONNECTOR_A));

        TableHandle insertTarget = createTableHandle(CONNECTOR_B);
        ColumnHandle column = new ColumnHandle() {};
        analysis.setInsert(new Analysis.Insert(insertTarget, ImmutableList.of(column)));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertEquals(connectors.size(), 2);
        assertTrue(connectors.contains(CONNECTOR_A));
        assertTrue(connectors.contains(CONNECTOR_B));
    }

    @Test
    public void testExtractConnectorsFromCreateTableDestination()
    {
        Analysis analysis = createAnalysis(null);
        analysis.registerTable(new Table(QualifiedName.of("source")), createTableHandle(CONNECTOR_A));
        analysis.setCreateTableDestination(new QualifiedObjectName("catalog_c", "schema", "new_table"));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertEquals(connectors.size(), 2);
        assertTrue(connectors.contains(CONNECTOR_A));
        assertTrue(connectors.contains(CONNECTOR_C));
    }

    @Test
    public void testExtractConnectorsCreateTableDestinationSameConnector()
    {
        Analysis analysis = createAnalysis(null);
        analysis.registerTable(new Table(QualifiedName.of("source")), createTableHandle(CONNECTOR_A));
        analysis.setCreateTableDestination(new QualifiedObjectName("catalog_a", "schema", "new_table"));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertEquals(connectors.size(), 1);
        assertTrue(connectors.contains(CONNECTOR_A));
    }

    @Test
    public void testExtractConnectorsCreateTableWithoutSource()
    {
        Analysis analysis = createAnalysis(null);
        analysis.setCreateTableDestination(new QualifiedObjectName("catalog_b", "schema", "new_table"));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertEquals(connectors.size(), 1);
        assertTrue(connectors.contains(CONNECTOR_B));
    }

    @Test
    public void testExtractConnectorsEmpty()
    {
        Analysis analysis = createAnalysis(null);

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertTrue(connectors.isEmpty());
    }

    @Test
    public void testExtractConnectorsAllSources()
    {
        Analysis analysis = createAnalysis(null);
        analysis.registerTable(new Table(QualifiedName.of("t1")), createTableHandle(CONNECTOR_A));

        TableHandle insertTarget = createTableHandle(CONNECTOR_B);
        ColumnHandle column = new ColumnHandle() {};
        analysis.setInsert(new Analysis.Insert(insertTarget, ImmutableList.of(column)));

        analysis.setCreateTableDestination(new QualifiedObjectName("catalog_c", "schema", "new_table"));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertEquals(connectors.size(), 3);
        assertTrue(connectors.contains(CONNECTOR_A));
        assertTrue(connectors.contains(CONNECTOR_B));
        assertTrue(connectors.contains(CONNECTOR_C));
    }

    @Test
    public void testExtractConnectorsDeduplicated()
    {
        Analysis analysis = createAnalysis(null);
        analysis.registerTable(new Table(QualifiedName.of("t1")), createTableHandle(CONNECTOR_A));
        analysis.registerTable(new Table(QualifiedName.of("t2")), createTableHandle(CONNECTOR_A));

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);
        Set<ConnectorId> connectors = queryAnalysis.extractConnectors();

        assertEquals(connectors.size(), 1);
        assertTrue(connectors.contains(CONNECTOR_A));
    }

    @Test
    public void testIsExplainAnalyzeQueryTrue()
    {
        Query innerQuery = simpleQuery();
        Explain explainAnalyze = new Explain(innerQuery, true, false, ImmutableList.of());
        Analysis analysis = createAnalysis(explainAnalyze);

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);

        assertTrue(queryAnalysis.isExplainAnalyzeQuery());
    }

    @Test
    public void testIsExplainAnalyzeQueryFalseWhenNotAnalyze()
    {
        Query innerQuery = simpleQuery();
        Explain explain = new Explain(innerQuery, false, false, ImmutableList.of());
        Analysis analysis = createAnalysis(explain);

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);

        assertFalse(queryAnalysis.isExplainAnalyzeQuery());
    }

    @Test
    public void testIsExplainAnalyzeQueryFalseWhenNotExplain()
    {
        Query query = simpleQuery();
        Analysis analysis = createAnalysis(query);

        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);

        assertFalse(queryAnalysis.isExplainAnalyzeQuery());
    }

    @Test
    public void testGetAnalysis()
    {
        Analysis analysis = createAnalysis(null);
        BuiltInQueryAnalysis queryAnalysis = new BuiltInQueryAnalysis(analysis);

        assertEquals(queryAnalysis.getAnalysis(), analysis);
    }

    private static Analysis createAnalysis(com.facebook.presto.sql.tree.Statement statement)
    {
        return new Analysis(statement, ImmutableMap.of(), false, new ViewDefinitionReferences());
    }

    private static TableHandle createTableHandle(ConnectorId connectorId)
    {
        return new TableHandle(connectorId, TABLE_HANDLE, TRANSACTION_HANDLE, Optional.empty());
    }

    private static Query simpleQuery()
    {
        return (Query) new com.facebook.presto.sql.parser.SqlParser().createStatement("SELECT 1");
    }
}
