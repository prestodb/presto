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
package com.facebook.presto.verifier;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.plugin.memory.MemoryPlugin;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.testing.mysql.MySqlOptions;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.checksum.ArrayColumnValidator;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.ColumnValidator;
import com.facebook.presto.verifier.checksum.FloatingPointColumnValidator;
import com.facebook.presto.verifier.checksum.MapColumnValidator;
import com.facebook.presto.verifier.checksum.RowColumnValidator;
import com.facebook.presto.verifier.checksum.SimpleColumnValidator;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.QueryObjectBundle;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.facebook.presto.verifier.source.MySqlSourceQueryConfig;
import com.facebook.presto.verifier.source.VerifierDao;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Provider;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static java.util.concurrent.TimeUnit.SECONDS;

public class VerifierTestUtil
{
    public static final String CATALOG = "verifier";
    public static final String SCHEMA = "default";
    public static final String XDB = "presto";
    public static final String VERIFIER_QUERIES_TABLE = "verifier_queries";

    public static final QueryObjectBundle TEST_BUNDLE = new QueryObjectBundle(
            QualifiedName.of("test"),
            ImmutableList.of(),
            new SqlParser(new SqlParserOptions().allowIdentifierSymbol(AT_SIGN, COLON)).createStatement(
                    "INSERT INTO test SELECT * FROM source",
                    ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build()),
            ImmutableList.of(),
            CONTROL);

    private static final MySqlOptions MY_SQL_OPTIONS = MySqlOptions.builder()
            .setCommandTimeout(new Duration(90, SECONDS))
            .build();

    private VerifierTestUtil() {}

    public static StandaloneQueryRunner setupPresto()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(session);
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog(CATALOG, "memory");
        return queryRunner;
    }

    public static TestingMySqlServer setupMySql()
            throws Exception
    {
        TestingMySqlServer mySqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of(XDB), MY_SQL_OPTIONS);
        try (Handle handle = getHandle(mySqlServer)) {
            handle.attach(VerifierDao.class).createVerifierQueriesTable(new MySqlSourceQueryConfig().getTableName());
        }
        return mySqlServer;
    }

    public static Handle getHandle(TestingMySqlServer mySqlServer)
    {
        return Jdbi.create(mySqlServer.getJdbcUrl(XDB)).installPlugin(new SqlObjectPlugin()).open();
    }

    public static void insertSourceQuery(Handle handle, String suite, String name, String query)
    {
        handle.execute(
                "INSERT INTO verifier_queries(\n" +
                        "    suite, name, control_catalog, control_schema, control_query, test_catalog, test_schema, test_query)\n" +
                        "SELECT\n" +
                        "    ?,\n" +
                        "    ?,\n" +
                        "    'verifier',\n" +
                        "    'default',\n" +
                        "    ?,\n" +
                        "    'verifier',\n" +
                        "    'default',\n" +
                        "    ?",
                suite,
                name,
                query,
                query);
    }

    public static void truncateVerifierQueries(Handle handle)
    {
        handle.execute("DELETE FROM verifier_queries");
    }

    public static ChecksumValidator createChecksumValidator(VerifierConfig verifierConfig)
    {
        Map<Column.Category, Provider<ColumnValidator>> lazyValidators = new HashMap<>();
        Map<Column.Category, Provider<ColumnValidator>> validators = ImmutableMap.of(
                Column.Category.SIMPLE, SimpleColumnValidator::new,
                Column.Category.FLOATING_POINT, () -> new FloatingPointColumnValidator(verifierConfig),
                Column.Category.ARRAY, ArrayColumnValidator::new,
                Column.Category.ROW, () -> new RowColumnValidator(lazyValidators),
                Column.Category.MAP, MapColumnValidator::new);
        lazyValidators.putAll(validators);
        return new ChecksumValidator(validators);
    }

    public static TypeManager createTypeManager()
    {
        return createTestFunctionAndTypeManager();
    }
}
