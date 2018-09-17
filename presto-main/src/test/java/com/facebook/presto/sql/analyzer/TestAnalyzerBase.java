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

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.ColumnPropertyManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.SchemaPropertyManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.assertions.Assert;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.connector.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.connector.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.testng.Assert.fail;

/**
 * Created by localadmin on 9/11/18.
 */
public class TestAnalyzerBase
{
    private static final String TPCH_CATALOG = "tpch";
    private static final ConnectorId TPCH_CONNECTOR_ID = new ConnectorId(TPCH_CATALOG);
    protected static final String SECOND_CATALOG = "c2";
    private static final ConnectorId SECOND_CONNECTOR_ID = new ConnectorId(SECOND_CATALOG);
    private static final String THIRD_CATALOG = "c3";
    private static final ConnectorId THIRD_CONNECTOR_ID = new ConnectorId(THIRD_CATALOG);
    private static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog("c1")
            .setSchema("s1")
            .build();
    protected static final Session CLIENT_SESSION = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("s1")
            .build();

    protected static final SqlParser SQL_PARSER = new SqlParser();

    protected TransactionManager transactionManager;
    protected AccessControl accessControl;
    protected Metadata metadata;
    protected ViewDefinition viewDefinition1;

    public static final TestFilteringMaskingAccessControl INSTANCE = new TestFilteringMaskingAccessControl();

    @BeforeClass
    public void setup()
    {
        TypeManager typeManager = new TypeRegistry();
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AccessControlManager(transactionManager);

        metadata = new MetadataManager(
                new FeaturesConfig(),
                typeManager,
                new BlockEncodingManager(typeManager),
                new SessionPropertyManager(),
                new SchemaPropertyManager(),
                new TablePropertyManager(),
                new ColumnPropertyManager(),
                transactionManager);

        metadata.getFunctionRegistry().addFunctions(ImmutableList.of(APPLY_FUNCTION));

        catalogManager.registerCatalog(createTestingCatalog(TPCH_CATALOG, TPCH_CONNECTOR_ID));
        catalogManager.registerCatalog(createTestingCatalog(SECOND_CATALOG, SECOND_CONNECTOR_ID));
        catalogManager.registerCatalog(createTestingCatalog(THIRD_CATALOG, THIRD_CONNECTOR_ID));

        SchemaTableName table1 = new SchemaTableName("s1", "t1");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table1,
                        ImmutableList.of(
                                new ColumnMetadata("a", BIGINT),
                                new ColumnMetadata("b", BIGINT),
                                new ColumnMetadata("c", BIGINT),
                                new ColumnMetadata("d", BIGINT))),
                false));

        SchemaTableName table2 = new SchemaTableName("s1", "t2");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table2,
                        ImmutableList.of(
                                new ColumnMetadata("a", BIGINT),
                                new ColumnMetadata("b", BIGINT))),
                false));

        SchemaTableName table3 = new SchemaTableName("s1", "t3");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table3,
                        ImmutableList.of(
                                new ColumnMetadata("a", BIGINT),
                                new ColumnMetadata("b", BIGINT),
                                new ColumnMetadata("x", BIGINT, null, true))),
                false));

        // table in different catalog
        SchemaTableName table4 = new SchemaTableName("s2", "t4");
        inSetupTransaction(session -> metadata.createTable(session, SECOND_CATALOG, new ConnectorTableMetadata(table4,
                        ImmutableList.of(
                                new ColumnMetadata("a", BIGINT))),
                false));

        // table with a hidden column
        SchemaTableName table5 = new SchemaTableName("s1", "t5");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table5,
                        ImmutableList.of(
                                new ColumnMetadata("a", BIGINT),
                                new ColumnMetadata("b", BIGINT, null, true))),
                false));

        // table with a varchar column
        SchemaTableName table6 = new SchemaTableName("s1", "t6");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table6,
                        ImmutableList.of(
                                new ColumnMetadata("a", BIGINT),
                                new ColumnMetadata("b", VARCHAR),
                                new ColumnMetadata("c", BIGINT),
                                new ColumnMetadata("d", BIGINT))),
                false));

        // table with bigint, double, array of bigints and array of doubles column
        SchemaTableName table7 = new SchemaTableName("s1", "t7");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG, new ConnectorTableMetadata(table7,
                        ImmutableList.of(
                                new ColumnMetadata("a", BIGINT),
                                new ColumnMetadata("b", DOUBLE),
                                new ColumnMetadata("c", new ArrayType(BIGINT)),
                                new ColumnMetadata("d", new ArrayType(DOUBLE)))),
                false));

        // valid view referencing table in same schema

        viewDefinition1 = new ViewDefinition(
                "select a from t1",
                Optional.of(TPCH_CATALOG),
                Optional.of("s1"),
                ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                Optional.of("user"));
        String viewData1 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(viewDefinition1);
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v1"), viewData1, false));

        // stale view (different column type)
        String viewData2 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", VARCHAR)),
                        Optional.of("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v2"), viewData2, false));

        // view referencing table in different schema from itself and session
        String viewData3 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t4",
                        Optional.of(SECOND_CATALOG),
                        Optional.of("s2"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                        Optional.of("owner")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(THIRD_CATALOG, "s3", "v3"), viewData3, false));

        // valid view with uppercase column name
        String viewData4 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select A from t1",
                        Optional.of("tpch"),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                        Optional.of("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName("tpch", "s1", "v4"), viewData4, false));

        // recursive view referencing to itself
        String viewData5 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select * from v5",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                        Optional.of("user")));
        inSetupTransaction(session -> metadata.createView(session, new QualifiedObjectName(TPCH_CATALOG, "s1", "v5"), viewData5, false));
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    private static Analyzer createAnalyzer(Session session, Metadata metadata, AccessControl accessControl)
    {
        return new Analyzer(
                session,
                metadata,
                SQL_PARSER,
                accessControl,
                Optional.empty(),
                emptyList());
    }

    private static Analyzer createAnalyzer(Session session, Metadata metadata)
    {
        return createAnalyzer(session, metadata, new AllowAllAccessControl());
    }

    protected void analyze(@Language("SQL") String query)
    {
        analyze(CLIENT_SESSION, query);
    }

    protected void analyzeWithRowColRewrite(@Language("SQL") String query, @Language("SQL") String expectedQuery)
    {
        analyze(CLIENT_SESSION, query, expectedQuery, INSTANCE);
    }

    protected void analyzeWithRowColRewrite(@Language("SQL") String query)
    {
        String queryRewritten = query.replaceAll("t1,", " t1 ")
                .replaceAll("t2,", " t2 ")
                .replaceAll(" t1 ", " ((select a,ceil(b) b,c,d from t1 where abs(a)>3) t1) ")
                .replaceAll(" s1\\.t1 ", " ((select a,ceil(b) b,c,d from s1.t1 where abs(a)>3) \"s1.t1\") ")
                .replaceAll(" s1\\.t2 ", " ((select a,b  from s1.t2 where abs(b)>3) \"s1.t2\") ")
                .replaceAll(" t2 ", " ((select a,b  from t2 where abs(b)>3) t2 )");

        analyze(CLIENT_SESSION, query, queryRewritten, INSTANCE);
    }

    protected void analyze(Session clientSession, @Language("SQL") String query)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(clientSession, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata);
                    Statement statement = SQL_PARSER.createStatement(query);
                    analyzer.analyze(statement);
                });
    }

    private void analyze(Session clientSession, @Language("SQL") String query, String modifiedStatement, AccessControl accessControl)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(clientSession, session -> {
                    Analyzer analyzer = createAnalyzer(session, metadata, accessControl);
                    Statement statement = SQL_PARSER.createStatement(query);
                    Assert.assertEquals(analyzer.analyze(statement).getStatement(), SQL_PARSER.createStatement(modifiedStatement));
                });
    }

    protected void assertFails(SemanticErrorCode error, @Language("SQL") String query)
    {
        assertFails(CLIENT_SESSION, error, query);
    }

    protected void assertFails(SemanticErrorCode error, int line, int column, @Language("SQL") String query)
    {
        assertFails(CLIENT_SESSION, error, Optional.of(new NodeLocation(line, column - 1)), query);
    }

    protected void assertFails(SemanticErrorCode error, String message, @Language("SQL") String query)
    {
        assertFails(CLIENT_SESSION, error, message, query);
    }

    protected void assertFails(Session session, SemanticErrorCode error, @Language("SQL") String query)
    {
        assertFails(session, error, Optional.empty(), query);
    }

    private void assertFails(Session session, SemanticErrorCode error, Optional<NodeLocation> location, @Language("SQL") String query)
    {
        try {
            analyze(session, query);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }

            if (location.isPresent()) {
                NodeLocation expected = location.get();
                NodeLocation actual = e.getNode().getLocation().get();

                if (expected.getLineNumber() != actual.getLineNumber() || expected.getColumnNumber() != actual.getColumnNumber()) {
                    fail(format(
                            "Expected error '%s' to occur at line %s, offset %s, but was: line %s, offset %s",
                            e.getCode(),
                            expected.getLineNumber(),
                            expected.getColumnNumber(),
                            actual.getLineNumber(),
                            actual.getColumnNumber()));
                }
            }
        }
    }

    protected void assertFails(Session session, SemanticErrorCode error, String message, @Language("SQL") String query)
    {
        try {
            analyze(session, query);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }

            if (!e.getMessage().matches(message)) {
                fail(format("Expected error '%s', but got '%s'", message, e.getMessage()), e);
            }
        }
    }

    private Catalog createTestingCatalog(String catalogName, ConnectorId connectorId)
    {
        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        Connector connector = createTestingConnector();
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                connectorId,
                connector,
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl),
                systemId,
                new SystemConnector(
                        systemId,
                        nodeManager,
                        connector.getSystemTables(),
                        transactionId -> transactionManager.getConnectorTransaction(transactionId, connectorId)));
    }

    private static Connector createTestingConnector()
    {
        return new Connector()
        {
            private final ConnectorMetadata metadata = new TestingMetadata();

            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return metadata;
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
