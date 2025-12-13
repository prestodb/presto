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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.connector.informationSchema.InformationSchemaConnector;
import com.facebook.presto.connector.system.SystemConnector;
import com.facebook.presto.connector.tvf.TestingTableFunctions.DescriptorArgumentFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.MonomorphicStaticReturnTypeFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.OnlyPassThroughFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.PassThroughFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.PolymorphicStaticReturnTypeFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.RequiredColumnsFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.SimpleTableFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.TableArgumentFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.TableArgumentRowSemanticsFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.TwoScalarArgumentsFunction;
import com.facebook.presto.connector.tvf.TestingTableFunctions.TwoTableArgumentsFunction;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.functionNamespace.testing.InMemoryFunctionNamespaceManager;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.procedure.BaseProcedure;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.TestProcedureRegistry;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingWarningCollector;
import com.facebook.presto.testing.TestingWarningCollectorConfig;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.CHECK_ACCESS_CONTROL_ON_UTILIZED_COLUMNS_ONLY;
import static com.facebook.presto.SystemSessionProperties.CHECK_ACCESS_CONTROL_WITH_SUBFIELDS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.SQL;
import static com.facebook.presto.spi.function.RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.TABLE_NAME;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.fail;

public class AbstractAnalyzerTest
{
    protected static final SqlParser SQL_PARSER = new SqlParser();
    protected static final String TPCH_CATALOG = "tpch";
    protected static final ConnectorId TPCH_CONNECTOR_ID = new ConnectorId(TPCH_CATALOG);
    protected static final String SECOND_CATALOG = "c2";
    protected static final ConnectorId SECOND_CONNECTOR_ID = new ConnectorId(SECOND_CATALOG);
    protected static final String THIRD_CATALOG = "c3";
    protected static final ConnectorId THIRD_CONNECTOR_ID = new ConnectorId(THIRD_CATALOG);

    protected static final Session SETUP_SESSION = testSessionBuilder()
            .setCatalog("c1")
            .setSchema("s1")
            .build();
    protected static final Session CLIENT_SESSION = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema("s1")
            .setSystemProperty(CHECK_ACCESS_CONTROL_ON_UTILIZED_COLUMNS_ONLY, "true")
            .setSystemProperty(CHECK_ACCESS_CONTROL_WITH_SUBFIELDS, "true")
            .build();

    protected static final SqlInvokedFunction SQL_FUNCTION_SQUARE = new SqlInvokedFunction(
            QualifiedObjectName.valueOf("unittest", "memory", "square"),
            ImmutableList.of(new Parameter("x", parseTypeSignature(StandardTypes.BIGINT))),
            parseTypeSignature(StandardTypes.BIGINT),
            "square",
            RoutineCharacteristics.builder()
                    .setDeterminism(DETERMINISTIC)
                    .setNullCallClause(RETURNS_NULL_ON_NULL_INPUT)
                    .build(),
            "RETURN x * x",
            notVersioned());

    protected TransactionManager transactionManager;
    protected AccessControl accessControl;
    protected Metadata metadata;

    @BeforeClass
    public void setup()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new TestingAccessControlManager(transactionManager);
        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig(), new FunctionsConfig(), new TestProcedureRegistry());

        metadata.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of(APPLY_FUNCTION));

        metadata.getFunctionAndTypeManager().addFunctionNamespace(
                "unittest",
                new InMemoryFunctionNamespaceManager(
                        "unittest",
                        new SqlFunctionExecutors(
                                ImmutableMap.of(SQL, FunctionImplementationType.SQL),
                                new NoopSqlFunctionExecutor()),
                        new SqlInvokedFunctionNamespaceManagerConfig().setSupportedFunctionLanguages("sql")));

        metadata.getFunctionAndTypeManager().createFunction(SQL_FUNCTION_SQUARE, true);

        metadata.getFunctionAndTypeManager().getTableFunctionRegistry().addTableFunctions(TPCH_CONNECTOR_ID,
                ImmutableList.of(
                        new SimpleTableFunction(),
                        new TwoScalarArgumentsFunction(),
                        new TableArgumentFunction(),
                        new DescriptorArgumentFunction(),
                        new TableArgumentRowSemanticsFunction(),
                        new TwoTableArgumentsFunction(),
                        new OnlyPassThroughFunction(),
                        new MonomorphicStaticReturnTypeFunction(),
                        new PolymorphicStaticReturnTypeFunction(),
                        new PassThroughFunction(),
                        new RequiredColumnsFunction()));

        List<Argument> arguments = new ArrayList<>();
        arguments.add(new Argument(SCHEMA, StandardTypes.VARCHAR));
        arguments.add(new Argument(TABLE_NAME, StandardTypes.VARCHAR));

        List<DistributedProcedure.Argument> distributedArguments = new ArrayList<>();
        distributedArguments.add(new DistributedProcedure.Argument(SCHEMA, StandardTypes.VARCHAR));
        distributedArguments.add(new DistributedProcedure.Argument(TABLE_NAME, StandardTypes.VARCHAR));
        List<BaseProcedure<?>> procedures = new ArrayList<>();
        procedures.add(new Procedure("system", "procedure", arguments));
        procedures.add(new TableDataRewriteDistributedProcedure("system", "distributed_procedure",
                distributedArguments,
                (session, transactionContext, procedureHandle, fragments) -> null,
                (session, transactionContext, procedureHandle, fragments) -> {},
                ignored -> new TestProcedureRegistry.TestProcedureContext()));
        metadata.getProcedureRegistry().addProcedures(SECOND_CONNECTOR_ID, procedures);

        Catalog tpchTestCatalog = createTestingCatalog(TPCH_CATALOG, TPCH_CONNECTOR_ID);
        catalogManager.registerCatalog(tpchTestCatalog);
        metadata.getAnalyzePropertyManager().addProperties(TPCH_CONNECTOR_ID, tpchTestCatalog.getConnector(TPCH_CONNECTOR_ID).getAnalyzeProperties());

        catalogManager.registerCatalog(createTestingCatalog(SECOND_CATALOG, SECOND_CONNECTOR_ID));
        catalogManager.registerCatalog(createTestingCatalog(THIRD_CATALOG, THIRD_CONNECTOR_ID));

        SchemaTableName table1 = new SchemaTableName("s1", "t1");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table1, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("c").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("d").setType(BIGINT).build())),
                false));

        SchemaTableName table2 = new SchemaTableName("s1", "t2");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table2, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(BIGINT).build())),
                false));

        SchemaTableName table3 = new SchemaTableName("s1", "t3");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table3, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("x").setType(BIGINT).setHidden(true).build())),
                false));

        // table in different catalog
        SchemaTableName table4 = new SchemaTableName("s2", "t4");
        inSetupTransaction(session -> metadata.createTable(session, SECOND_CATALOG,
                new ConnectorTableMetadata(table4, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build())),
                false));

        // table with a hidden column
        SchemaTableName table5 = new SchemaTableName("s1", "t5");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table5, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(BIGINT).setHidden(true).build())),
                false));

        // table with a varchar column
        SchemaTableName table6 = new SchemaTableName("s1", "t6");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table6, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(VARCHAR).build(),
                        ColumnMetadata.builder().setName("c").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("d").setType(BIGINT).build())),
                false));

        // table with bigint, double, array of bigints and array of doubles column
        SchemaTableName table7 = new SchemaTableName("s1", "t7");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table7, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(DOUBLE).build(),
                        ColumnMetadata.builder().setName("c").setType(new ArrayType(BIGINT)).build(),
                        ColumnMetadata.builder().setName("d").setType(new ArrayType(DOUBLE)).build())),
                false));

        // table with double, array of bigints, real, and bigint
        SchemaTableName table8 = new SchemaTableName("s1", "t8");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table8, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(DOUBLE).build(),
                        ColumnMetadata.builder().setName("b").setType(new ArrayType(BIGINT)).build(),
                        ColumnMetadata.builder().setName("c").setType(RealType.REAL).build(),
                        ColumnMetadata.builder().setName("d").setType(BIGINT).build())),
                false));

        // table with double, array of bigints, real, and bigint
        SchemaTableName table9 = new SchemaTableName("s1", "t9");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table9, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(DOUBLE).build(),
                        ColumnMetadata.builder().setName("b").setType(new ArrayType(BIGINT)).build(),
                        ColumnMetadata.builder().setName("c").setType(RealType.REAL).build(),
                        ColumnMetadata.builder().setName("d").setType(BIGINT).build())),
                false));

        // table with nested struct
        SchemaTableName table10 = new SchemaTableName("s1", "t10");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table10, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("w"), BIGINT),
                                new RowType.Field(Optional.of("x"),
                                        RowType.from(ImmutableList.of(
                                                new RowType.Field(Optional.of("y"), BIGINT),
                                                new RowType.Field(Optional.of("z"), DOUBLE))))))).build(),
                        ColumnMetadata.builder().setName("c").setType(RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("d"), BIGINT)))).build())),
                false));

        // table with nested arrays, structs
        SchemaTableName table11 = new SchemaTableName("s1", "t11");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table11, ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(new ArrayType(RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("x"), BIGINT),
                                new RowType.Field(Optional.of("y"), BIGINT))))).build(),
                        ColumnMetadata.builder().setName("b").setType(RowType.from(ImmutableList.of(
                                new RowType.Field(Optional.of("w"), BIGINT),
                                new RowType.Field(Optional.of("x"),
                                        new ArrayType(new ArrayType(RowType.from(ImmutableList.of(new RowType.Field(Optional.of("y"), BIGINT))))))))).build(),
                        ColumnMetadata.builder().setName("c").setType(RowType.from(ImmutableList.of(
                                new RowType.Field(
                                        Optional.of("x"),
                                        new ArrayType(RowType.from(ImmutableList.of(
                                                new RowType.Field(Optional.of("x"), BIGINT),
                                                new RowType.Field(Optional.of("y"), BIGINT)))))))).build())),
                false));

        // table with columns containing special characters
        SchemaTableName table12 = new SchemaTableName("s1", "t12");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table12, ImmutableList.of(
                        ColumnMetadata.builder().setName("a.x").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("a&^[x").setType(BIGINT).build())),
                false));

        // table with bigint, double, array of bigints and array of doubles column
        SchemaTableName table13 = new SchemaTableName("s1", "t13");
        inSetupTransaction(session -> metadata.createTable(session, TPCH_CATALOG,
                new ConnectorTableMetadata(table13, ImmutableList.of(
                        ColumnMetadata.builder().setName("w").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("x").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("y").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("z").setType(BIGINT).build())),
                false));

        // materialized view referencing table in same schema
        List<SchemaTableName> baseTables = new ArrayList<>(Collections.singletonList(table2));
        MaterializedViewDefinition.TableColumn baseTableColumns = new MaterializedViewDefinition.TableColumn(table2, "a", true);

        SchemaTableName materializedTable = new SchemaTableName("s1", "mv1");
        MaterializedViewDefinition.TableColumn materializedViewTableColumn = new MaterializedViewDefinition.TableColumn(materializedTable, "a", true);

        List<MaterializedViewDefinition.ColumnMapping> columnMappings = Collections.singletonList(
                new MaterializedViewDefinition.ColumnMapping(materializedViewTableColumn, Collections.singletonList(baseTableColumns)));

        MaterializedViewDefinition materializedViewData1 = new MaterializedViewDefinition(
                        "select a from t2",
                        "s1",
                        "mv1",
                        baseTables,
                        Optional.of("user"),
                        Optional.empty(),
                        columnMappings,
                        new ArrayList<>(),
                        Optional.of(new ArrayList<>(Collections.singletonList("a"))));

        ConnectorTableMetadata materializedViewMetadata1 = new ConnectorTableMetadata(
                materializedTable, ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));

        inSetupTransaction(session ->
                metadata.createMaterializedView(session, TPCH_CATALOG, materializedViewMetadata1, materializedViewData1, false));

        // valid view referencing table in same schema
        String viewData1 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                        Optional.of("user"),
                        false));
        ConnectorTableMetadata viewMetadata1 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "v1"),
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadata1, viewData1, false));

        // stale view (different column type)
        String viewData2 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", VARCHAR)),
                        Optional.of("user"),
                        false));
        ConnectorTableMetadata viewMetadata2 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "v2"),
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(VARCHAR).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadata2, viewData2, false));

        // view referencing table in different schema from itself and session
        String viewData3 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a from t4",
                        Optional.of(SECOND_CATALOG),
                        Optional.of("s2"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                        Optional.of("owner"),
                        false));
        ConnectorTableMetadata viewMetadata3 = new ConnectorTableMetadata(
                new SchemaTableName("s3", "v3"),
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, THIRD_CATALOG, viewMetadata3, viewData3, false));

        // valid view with uppercase column name
        String viewData4 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select A from t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                        Optional.of("user"),
                        false));
        ConnectorTableMetadata viewMetadata4 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "v4"),
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadata4, viewData4, false));

        // recursive view referencing to itself
        String viewData5 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select * from v5",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(new ViewDefinition.ViewColumn("a", BIGINT)),
                        Optional.of("user"),
                        false));
        ConnectorTableMetadata viewMetadata5 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "v5"),
                ImmutableList.of(ColumnMetadata.builder().setName("a").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadata5, viewData5, false));

        String viewDataInvoker1 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a,b,c from t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(
                                new ViewDefinition.ViewColumn("a", BIGINT),
                                new ViewDefinition.ViewColumn("b", BIGINT),
                                new ViewDefinition.ViewColumn("c", BIGINT)),
                        Optional.empty(),
                        true));

        ConnectorTableMetadata viewMetadataInvoker1 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "view_invoker1"),
                ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("c").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadataInvoker1, viewDataInvoker1, false));

        String viewDataInvoker2 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select x, y, z from t13",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(
                                new ViewDefinition.ViewColumn("x", BIGINT),
                                new ViewDefinition.ViewColumn("y", BIGINT),
                                new ViewDefinition.ViewColumn("z", BIGINT)),
                        Optional.empty(),
                        true));
        ConnectorTableMetadata viewMetadataInvoker2 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "view_invoker2"),
                ImmutableList.of(
                        ColumnMetadata.builder().setName("x").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("y").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("z").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadataInvoker2, viewDataInvoker2, false));

        String viewDataDefiner1 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select a,b,c from t1",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(
                                new ViewDefinition.ViewColumn("a", BIGINT),
                                new ViewDefinition.ViewColumn("b", BIGINT),
                                new ViewDefinition.ViewColumn("c", BIGINT)),
                        Optional.of("different_user"),
                        false));

        ConnectorTableMetadata viewMetadataDefiner1 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "view_definer1"),
                ImmutableList.of(
                        ColumnMetadata.builder().setName("a").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("b").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("c").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadataDefiner1, viewDataDefiner1, false));

        String viewDataDefiner2 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "select x, y, z from t13",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(
                                new ViewDefinition.ViewColumn("x", BIGINT),
                                new ViewDefinition.ViewColumn("y", BIGINT),
                                new ViewDefinition.ViewColumn("z", BIGINT)),
                        Optional.of("different_user"),
                        false));
        ConnectorTableMetadata viewMetadataDefiner2 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "view_definer2"),
                ImmutableList.of(
                        ColumnMetadata.builder().setName("x").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("y").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("z").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadataDefiner2, viewDataDefiner2, false));

        String viewDataInvokerWithCte1 = JsonCodec.jsonCodec(ViewDefinition.class).toJson(
                new ViewDefinition(
                        "WITH cte as (SELECT a + 1 as c1, b +1 as c2, c +1 as c3 FROM t1) SELECT c1, c2, c3 from cte where c1 > 5",
                        Optional.of(TPCH_CATALOG),
                        Optional.of("s1"),
                        ImmutableList.of(
                                new ViewDefinition.ViewColumn("c1", BIGINT),
                                new ViewDefinition.ViewColumn("c2", BIGINT),
                                new ViewDefinition.ViewColumn("c3", BIGINT)),
                        Optional.empty(),
                        true));
        ConnectorTableMetadata viewMetadataInvokerWithCte1 = new ConnectorTableMetadata(
                new SchemaTableName("s1", "view_invoker_with_cte1"),
                ImmutableList.of(
                        ColumnMetadata.builder().setName("c1").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("c2").setType(BIGINT).build(),
                        ColumnMetadata.builder().setName("c3").setType(BIGINT).build()));
        inSetupTransaction(session -> metadata.createView(session, TPCH_CATALOG, viewMetadataInvokerWithCte1, viewDataInvokerWithCte1, false));
    }

    private void inSetupTransaction(Consumer<Session> consumer)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .execute(SETUP_SESSION, consumer);
    }

    protected void analyze(@Language("SQL") String query)
    {
        analyze(CLIENT_SESSION, query);
    }

    protected WarningCollector analyzeWithWarnings(@Language("SQL") String query)
    {
        WarningCollector warningCollector = new TestingWarningCollector(new WarningCollectorConfig(), new TestingWarningCollectorConfig());
        analyze(CLIENT_SESSION, warningCollector, query);
        return warningCollector;
    }

    protected void analyze(Session clientSession, @Language("SQL") String query)
    {
        analyze(clientSession, WarningCollector.NOOP, query);
    }

    private void analyze(Session clientSession, WarningCollector warningCollector, @Language("SQL") String query)
    {
        transaction(transactionManager, accessControl)
                .singleStatement()
                .readUncommitted()
                .readOnly()
                .execute(clientSession, session -> {
                    Analyzer analyzer = AbstractAnalyzerTest.createAnalyzer(session, metadata, warningCollector, query);
                    Statement statement = SQL_PARSER.createStatement(query);
                    analyzer.analyze(statement);
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
        assertFails(CLIENT_SESSION, error, message, query, false);
    }

    protected void assertFails(Session session, SemanticErrorCode error, @Language("SQL") String query)
    {
        assertFails(session, error, Optional.empty(), query);
    }

    protected void assertFails(Session session, SemanticErrorCode error, String message, @Language("SQL") String query)
    {
        assertFails(session, error, message, query, false);
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
                NodeLocation actual = e.getLocation().get();

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

    protected void assertFails(Session session, SemanticErrorCode error, String message, @Language("SQL") String query, boolean exact)
    {
        try {
            analyze(session, query);
            fail(format("Expected error %s, but analysis succeeded", error));
        }
        catch (SemanticException e) {
            if (e.getCode() != error) {
                fail(format("Expected error %s, but found %s: %s", error, e.getCode(), e.getMessage()), e);
            }

            if (!(exact ? e.getMessage().equals(message) : e.getMessage().matches(message))) {
                fail(format("Expected error '%s', but got '%s'", message, e.getMessage()), e);
            }
        }
    }

    protected static Analyzer createAnalyzer(Session session, Metadata metadata, WarningCollector warningCollector, String query)
    {
        return new Analyzer(
                session,
                metadata,
                SQL_PARSER,
                new AllowAllAccessControl(),
                Optional.empty(),
                emptyList(),
                emptyMap(),
                warningCollector,
                query);
    }

    private Catalog createTestingCatalog(String catalogName, ConnectorId connectorId)
    {
        ConnectorId systemId = createSystemTablesConnectorId(connectorId);
        Connector connector = AbstractAnalyzerTest.createTestingConnector();
        InternalNodeManager nodeManager = new InMemoryNodeManager();
        return new Catalog(
                catalogName,
                connectorId,
                connector,
                createInformationSchemaConnectorId(connectorId),
                new InformationSchemaConnector(catalogName, nodeManager, metadata, accessControl, ImmutableList.of()),
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

            @Override
            public List<PropertyMetadata<?>> getAnalyzeProperties()
            {
                return ImmutableList.of(
                        stringProperty("p1", "test string property", "", false),
                        integerProperty("p2", "test integer property", 0, false));
            }
        };
    }
}
