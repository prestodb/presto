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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.teradata.tempto.AfterTestWithContext;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.ldap.LdapObjectRequirement;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.AMERICA_ORG;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.ASIA_ORG;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.CHILD_GROUP;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.CHILD_GROUP_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.DEFAULT_GROUP;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.DEFAULT_GROUP_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.ORPHAN_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.PARENT_GROUP;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.PARENT_GROUP_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.SPECIAL_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.USER_IN_MULTIPLE_GROUPS;
import static com.facebook.presto.tests.TestGroups.LDAP;
import static com.facebook.presto.tests.TestGroups.LDAP_CLI;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.tempto.process.CliProcess.trimLines;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class PrestoLdapCliTests
        extends PrestoCliLauncher
        implements RequirementsProvider
{
    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_truststore_path")
    private String ldapTruststorePath;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_truststore_password")
    private String ldapTruststorePassword;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_user_name")
    private String ldapUserName;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_server_address")
    private String ldapServerAddress;

    @Inject(optional = true)
    @Named("databases.presto.cli_ldap_user_password")
    private String ldapUserPassword;

    public PrestoLdapCliTests()
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
        return compose(new LdapObjectRequirement(
                        Arrays.asList(
                                AMERICA_ORG, ASIA_ORG,
                                DEFAULT_GROUP, PARENT_GROUP, CHILD_GROUP,
                                DEFAULT_GROUP_USER, PARENT_GROUP_USER, CHILD_GROUP_USER, ORPHAN_USER, SPECIAL_USER, USER_IN_MULTIPLE_GROUPS)),
                immutableTable(NATION));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunQueryWithLdap()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument();
        presto.waitForPrompt();
        presto.getProcessInput().println("select * from hive.default.nation;");
        assertThat(trimLines(presto.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunBatchQueryWithLdap()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunQueryFromFileWithLdap()
            throws IOException, InterruptedException
    {
        File temporayFile = File.createTempFile("test-sql", null);
        temporayFile.deleteOnExit();
        Files.write("select * from hive.default.nation;\n", temporayFile, UTF_8);

        launchPrestoCliWithServerArgument("--file", temporayFile.getAbsolutePath());
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldPassQueryForLdapUserInMultipleGroups()
            throws IOException, InterruptedException
    {
        ldapUserName = USER_IN_MULTIPLE_GROUPS.getAttributes().get("cn");

        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapUserInChildGroup()
            throws IOException, InterruptedException
    {
        ldapUserName = CHILD_GROUP_USER.getAttributes().get("cn");
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("User " + ldapUserName + " not a member of the authorized group")));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapUserInParentGroup()
            throws IOException, InterruptedException
    {
        ldapUserName = PARENT_GROUP_USER.getAttributes().get("cn");
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("User " + ldapUserName + " not a member of the authorized group")));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForOrphanLdapUser()
            throws IOException, InterruptedException
    {
        ldapUserName = ORPHAN_USER.getAttributes().get("cn");
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("User " + ldapUserName + " not a member of the authorized group")));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForWrongLdapPassword()
            throws IOException, InterruptedException
    {
        ldapUserPassword = "wrong_password";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Invalid credentials")));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForWrongLdapUser()
            throws IOException, InterruptedException
    {
        ldapUserName = "invalid_user";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Invalid credentials")));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForEmptyUser()
            throws IOException, InterruptedException
    {
        ldapUserName = "";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Malformed decoded credentials")));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapWithoutPassword()
            throws IOException, InterruptedException
    {
        launchPrestoCli("--server", ldapServerAddress,
                "--truststore-path", ldapTruststorePath,
                "--truststore-password", ldapTruststorePassword,
                "--user", ldapUserName,
                "--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Authentication failed: Unauthorized")));
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapWithoutHttps()
            throws IOException, InterruptedException
    {
        ldapServerAddress = format("http://%s:8443", serverHost);
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Authentication using username/password requires HTTPS to be enabled")));
        skipAfterTestWithContext();
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailForIncorrectTrustStore()
            throws IOException, InterruptedException
    {
        ldapTruststorePassword = "wrong_password";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Keystore was tampered with, or password was incorrect")));
        skipAfterTestWithContext();
    }

    private void skipAfterTestWithContext()
    {
        presto.close();
        presto = null;
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldPassForCredentialsWithSpecialCharacters()
            throws IOException, InterruptedException
    {
        ldapUserName = SPECIAL_USER.getAttributes().get("cn");
        ldapUserPassword = SPECIAL_USER.getAttributes().get("userPassword");
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertThat(trimLines(presto.readRemainingOutputLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = {LDAP, LDAP_CLI, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailForUserWithColon()
            throws IOException, InterruptedException
    {
        ldapUserName = "UserWith:Colon";
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertTrue(trimLines(presto.readRemainingErrorLines()).stream().anyMatch(str -> str.contains("Illegal character ':' found in username")));
        skipAfterTestWithContext();
    }

    private void launchPrestoCliWithServerArgument(String... arguments)
            throws IOException, InterruptedException
    {
        requireNonNull(ldapTruststorePath, "databases.presto.cli_ldap_truststore_path is null");
        requireNonNull(ldapTruststorePassword, "databases.presto.cli_ldap_truststore_password is null");
        requireNonNull(ldapUserName, "databases.presto.cli_ldap_user_name is null");
        requireNonNull(ldapServerAddress, "databases.presto.cli_ldap_server_address is null");
        requireNonNull(ldapUserPassword, "databases.presto.cli_ldap_user_password is null");

        ImmutableList.Builder<String> prestoClientOptions = ImmutableList.builder();
        prestoClientOptions.add(
                "--server", ldapServerAddress,
                "--truststore-path", ldapTruststorePath,
                "--truststore-password", ldapTruststorePassword,
                "--user", ldapUserName,
                "--password");

        prestoClientOptions.add(arguments);
        ProcessBuilder processBuilder = getProcessBuilder(prestoClientOptions.build());
        processBuilder.environment().put("PRESTO_PASSWORD", ldapUserPassword);
        presto = new PrestoCliProcess(processBuilder.start());
    }
}
