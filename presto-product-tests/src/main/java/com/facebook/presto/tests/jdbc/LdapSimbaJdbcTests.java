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
package com.facebook.presto.tests.jdbc;

import io.prestodb.tempto.Requires;
import io.prestodb.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.CHILD_GROUP_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.ORPHAN_USER;
import static com.facebook.presto.tests.ImmutableLdapObjectDefinitions.PARENT_GROUP_USER;
import static com.facebook.presto.tests.TestGroups.LDAP;
import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.TestGroups.SIMBA_JDBC;
import static com.facebook.presto.tests.TpchTableResults.PRESTO_NATION_RESULT;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class LdapSimbaJdbcTests
        extends LdapJdbcTests
{
    private static final String JDBC_URL_FORMAT = "jdbc:presto://%s;AuthenticationType=LDAP Authentication;" +
            "SSLTrustStorePath=%s;SSLTrustStorePwd=%s;AllowSelfSignedServerCert=1;AllowHostNameCNMismatch=1";
    private static final String SSL_CERTIFICATE_ERROR =
            "[Teradata][Presto](100140) SSL certificate error: Keystore was tampered with, or password was incorrect.";
    private static final String INVALID_CREDENTIALS_ERROR =
            "[Teradata][Presto](100240) Authentication failed: Invalid credentials.";
    private static final String MALFORMED_CREDENTIALS_ERROR =
            "[Teradata][Presto](100240) Authentication failed: Malformed decoded credentials.";
    private static final String UNAUTHORIZED_USER_ERROR =
            "[Teradata][Presto](100240) Authentication failed: Unauthorized user.";
    private static final String INVALID_SSL_PROPERTY =
            "[Teradata][Presto](100200) Connection string is invalid: SSL value is not valid for given AuthenticationType.";

    @Override
    protected String getLdapUrlFormat()
    {
        return JDBC_URL_FORMAT;
    }

    @Requires(ImmutableNationTable.class)
    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldRunQueryWithLdap()
            throws SQLException
    {
        assertThat(executeLdapQuery(NATION_SELECT_ALL_QUERY, ldapUserName, ldapUserPassword)).matches(PRESTO_NATION_RESULT);
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapUserInChildGroup()
    {
        String name = CHILD_GROUP_USER.getAttributes().get("cn");
        expectQueryToFailForUserNotInGroup(name);
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapUserInParentGroup()
    {
        String name = PARENT_GROUP_USER.getAttributes().get("cn");
        expectQueryToFailForUserNotInGroup(name);
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForOrphanLdapUser()
    {
        String name = ORPHAN_USER.getAttributes().get("cn");
        expectQueryToFailForUserNotInGroup(name);
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForWrongLdapPassword()
    {
        expectQueryToFail(ldapUserName, "wrong_password", INVALID_CREDENTIALS_ERROR);
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForWrongLdapUser()
    {
        expectQueryToFail("invalid_user", ldapUserPassword, INVALID_CREDENTIALS_ERROR);
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForEmptyUser()
    {
        expectQueryToFail("", ldapUserPassword, MALFORMED_CREDENTIALS_ERROR);
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapWithoutPassword()
    {
        expectQueryToFail(ldapUserName, "", MALFORMED_CREDENTIALS_ERROR);
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailQueryForLdapWithoutSsl()
    {
        try {
            DriverManager.getConnection(getLdapUrl() + ";SSL=0", ldapUserName, ldapUserPassword);
            fail();
        }
        catch (SQLException exception) {
            assertEquals(exception.getMessage(), INVALID_SSL_PROPERTY);
        }
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailForIncorrectTrustStore()
    {
        try {
            String url = String.format(JDBC_URL_FORMAT, prestoServer(), ldapTruststorePath, "wrong_password");
            Connection connection = DriverManager.getConnection(url, ldapUserName, ldapUserPassword);
            Statement statement = connection.createStatement();
            statement.executeQuery(NATION_SELECT_ALL_QUERY);
            fail();
        }
        catch (SQLException exception) {
            assertEquals(exception.getMessage(), SSL_CERTIFICATE_ERROR);
        }
    }

    @Test(groups = {LDAP, SIMBA_JDBC, PROFILE_SPECIFIC_TESTS}, timeOut = TIMEOUT)
    public void shouldFailForUserWithColon()
    {
        expectQueryToFail("UserWith:Colon", ldapUserPassword, MALFORMED_CREDENTIALS_ERROR);
    }

    private void expectQueryToFailForUserNotInGroup(String user)
    {
        expectQueryToFail(user, ldapUserPassword, UNAUTHORIZED_USER_ERROR);
    }
}
