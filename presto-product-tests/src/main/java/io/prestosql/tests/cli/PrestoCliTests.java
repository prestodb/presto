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
package com.facebook.presto.tests.cli;

import com.facebook.presto.cli.Presto;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.testing.TempFile;
import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.tests.TestGroups.CLI;
import static com.google.common.base.MoreObjects.firstNonNull;
import static io.prestodb.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestodb.tempto.process.CliProcess.trimLines;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PrestoCliTests
        extends PrestoCliLauncher
        implements RequirementsProvider
{
    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_authentication")
    private boolean kerberosAuthentication;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_principal")
    private String kerberosPrincipal;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_keytab")
    private String kerberosKeytab;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_config_path")
    private String kerberosConfigPath;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_service_name")
    private String kerberosServiceName;

    @Inject(optional = true)
    @Named("databases.presto.https_keystore_path")
    private String keystorePath;

    @Inject(optional = true)
    @Named("databases.presto.https_keystore_password")
    private String keystorePassword;

    @Inject(optional = true)
    @Named("databases.presto.cli_kerberos_use_canonical_hostname")
    private boolean kerberosUseCanonicalHostname;

    @Inject
    @Named("databases.presto.jdbc_user")
    private String jdbcUser;

    public PrestoCliTests()
            throws IOException
    {}

    @AfterTestWithContext
    public void stopPresto()
            throws InterruptedException
    {
        super.stopPresto();
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return new ImmutableTableRequirement(NATION);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldDisplayVersion()
            throws IOException
    {
        launchPrestoCli("--version");
        String version = firstNonNull(Presto.class.getPackage().getImplementationVersion(), "(version unknown)");
        assertThat(presto.readRemainingOutputLines()).containsExactly("Presto CLI " + version);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunQuery()
            throws IOException
    {
        launchPrestoCliWithServerArgument();
        presto.waitForPrompt();
        presto.getProcessInput().println("select * from hive.default.nation;");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunBatchQuery()
            throws Exception
    {
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
        presto.waitForWithTimeoutAndKill();
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldUseCatalogAndSchemaOptions()
            throws Exception
    {
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
        presto.waitForWithTimeoutAndKill();
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunQueryFromFile()
            throws Exception
    {
        try (TempFile file = new TempFile()) {
            Files.write("select * from hive.default.nation;\n", file.file(), UTF_8);

            launchPrestoCliWithServerArgument("--file", file.file().getAbsolutePath());
            assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);

            presto.waitForWithTimeoutAndKill();
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldExitOnErrorFromExecute()
            throws IOException, InterruptedException
    {
        String sql = "select * from hive.default.nations; select * from hive.default.nation;";
        launchPrestoCliWithServerArgument("--execute", sql);
        assertThat(trimLines(presto.readRemainingOutputLines())).isEmpty();

        assertThatThrownBy(() -> presto.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldExitOnErrorFromFile()
            throws IOException, InterruptedException
    {
        try (TempFile file = new TempFile()) {
            Files.write("select * from hive.default.nations;\nselect * from hive.default.nation;\n", file.file(), UTF_8);

            launchPrestoCliWithServerArgument("--file", file.file().getAbsolutePath());
            assertThat(trimLines(presto.readRemainingOutputLines())).isEmpty();

            assertThatThrownBy(() -> presto.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldNotExitOnErrorFromExecute()
            throws IOException, InterruptedException
    {
        String sql = "select * from hive.default.nations; select * from hive.default.nation;";
        launchPrestoCliWithServerArgument("--execute", sql, "--ignore-errors");
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);

        assertThatThrownBy(() -> presto.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldNotExitOnErrorFromFile()
            throws IOException, InterruptedException
    {
        try (TempFile file = new TempFile()) {
            Files.write("select * from hive.default.nations;\nselect * from hive.default.nation;\n", file.file(), UTF_8);

            launchPrestoCliWithServerArgument("--file", file.file().getAbsolutePath(), "--ignore-errors");
            assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);

            assertThatThrownBy(() -> presto.waitForWithTimeoutAndKill()).hasMessage("Child process exited with non-zero code: 1");
        }
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldHandleSession()
            throws IOException
    {
        launchPrestoCliWithServerArgument();
        presto.waitForPrompt();

        presto.getProcessInput().println("use hive.default;");
        assertThat(presto.readLinesUntilPrompt()).contains("USE");

        presto.getProcessInput().println("select * from nation;");
        assertThat(trimLines(presto.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);

        presto.getProcessInput().println("show session;");
        assertThat(squeezeLines(presto.readLinesUntilPrompt()))
                .contains("join_distribution_type|PARTITIONED|PARTITIONED|varchar|The join method to use. Options are BROADCAST,PARTITIONED,AUTOMATIC");

        presto.getProcessInput().println("set session join_distribution_type = 'BROADCAST';");
        assertThat(presto.readLinesUntilPrompt()).contains("SET SESSION");

        presto.getProcessInput().println("show session;");
        assertThat(squeezeLines(presto.readLinesUntilPrompt()))
                .contains("join_distribution_type|BROADCAST|PARTITIONED|varchar|The join method to use. Options are BROADCAST,PARTITIONED,AUTOMATIC");
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldHandleTransaction()
            throws IOException
    {
        launchPrestoCliWithServerArgument();
        presto.waitForPrompt();

        presto.getProcessInput().println("use hive.default;");
        assertThat(presto.readLinesUntilPrompt()).contains("USE");

        // start transaction and create table
        presto.getProcessInput().println("start transaction;");
        assertThat(presto.readLinesUntilPrompt()).contains("START TRANSACTION");

        presto.getProcessInput().println("create table txn_test (x bigint);");
        assertThat(presto.readLinesUntilPrompt()).contains("CREATE TABLE");

        // cause an error that aborts the transaction
        presto.getProcessInput().println("select foo;");
        assertThat(presto.readLinesUntilPrompt()).extracting(PrestoCliTests::removePrefix)
                .contains("line 1:8: Column 'foo' cannot be resolved");

        // verify commands are rejected until rollback
        presto.getProcessInput().println("select * from nation;");
        assertThat(presto.readLinesUntilPrompt()).extracting(PrestoCliTests::removePrefix)
                .contains("Current transaction is aborted, commands ignored until end of transaction block");

        presto.getProcessInput().println("rollback;");
        assertThat(presto.readLinesUntilPrompt()).contains("ROLLBACK");

        // verify commands work after rollback
        presto.getProcessInput().println("select * from nation;");
        assertThat(trimLines(presto.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);

        // verify table was not created
        presto.getProcessInput().println("show tables;");
        assertThat(trimLines(presto.readLinesUntilPrompt())).doesNotContain("txn_test");

        // start transaction, create two tables and commit
        presto.getProcessInput().println("start transaction;");
        assertThat(presto.readLinesUntilPrompt()).contains("START TRANSACTION");

        presto.getProcessInput().println("create table txn_test1 (x bigint);");
        assertThat(presto.readLinesUntilPrompt()).contains("CREATE TABLE");

        presto.getProcessInput().println("create table txn_test2 (x bigint);");
        assertThat(presto.readLinesUntilPrompt()).contains("CREATE TABLE");

        presto.getProcessInput().println("commit;");
        assertThat(presto.readLinesUntilPrompt()).contains("COMMIT");

        // verify tables were created
        presto.getProcessInput().println("show tables;");
        assertThat(trimLines(presto.readLinesUntilPrompt())).contains("txn_test1", "txn_test2");
    }

    private void launchPrestoCliWithServerArgument(String... arguments)
            throws IOException
    {
        ImmutableList.Builder<String> prestoClientOptions = ImmutableList.builder();
        prestoClientOptions.add("--server", serverAddress);
        prestoClientOptions.add("--user", jdbcUser);

        if (keystorePath != null) {
            prestoClientOptions.add("--keystore-path", keystorePath);
        }

        if (keystorePassword != null) {
            prestoClientOptions.add("--keystore-password", keystorePassword);
        }

        if (kerberosAuthentication) {
            requireNonNull(kerberosPrincipal, "databases.presto.cli_kerberos_principal is null");
            requireNonNull(kerberosKeytab, "databases.presto.cli_kerberos_keytab is null");
            requireNonNull(kerberosServiceName, "databases.presto.cli_kerberos_service_name is null");
            requireNonNull(kerberosConfigPath, "databases.presto.cli_kerberos_config_path is null");

            prestoClientOptions.add("--krb5-principal", kerberosPrincipal);
            prestoClientOptions.add("--krb5-keytab-path", kerberosKeytab);
            prestoClientOptions.add("--krb5-remote-service-name", kerberosServiceName);
            prestoClientOptions.add("--krb5-config-path", kerberosConfigPath);

            if (!kerberosUseCanonicalHostname) {
                prestoClientOptions.add("--krb5-disable-remote-service-hostname-canonicalization");
            }
        }

        prestoClientOptions.add(arguments);
        launchPrestoCli(prestoClientOptions.build());
    }

    private static String removePrefix(String line)
    {
        int i = line.indexOf(':');
        return (i >= 0) ? line.substring(i + 1).trim() : line;
    }

    public static List<String> squeezeLines(List<String> lines)
    {
        return lines.stream()
                .map(line -> line.replaceAll(" +\\| +", "|").trim())
                .collect(toList());
    }
}
