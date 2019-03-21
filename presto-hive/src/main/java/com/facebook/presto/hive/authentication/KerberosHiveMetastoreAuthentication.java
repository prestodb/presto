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
package com.facebook.presto.hive.authentication;

import com.facebook.presto.hive.ForHiveMetastore;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;

import javax.inject.Inject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import static com.facebook.presto.hive.authentication.UserGroupInformationUtils.executeActionInDoAs;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static javax.security.sasl.Sasl.QOP;
import static javax.security.sasl.Sasl.SERVER_AUTH;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.KERBEROS;
import static org.apache.hadoop.security.SaslRpcServer.AuthMethod.TOKEN;
import static org.apache.hadoop.security.SaslRpcServer.SASL_DEFAULT_REALM;
import static org.apache.hadoop.security.SecurityUtil.getServerPrincipal;

public class KerberosHiveMetastoreAuthentication
        implements HiveMetastoreAuthentication
{
    private final String hiveMetastoreServicePrincipal;
    private final HadoopAuthentication authentication;
    private final boolean hdfsWireEncryptionEnabled;
    private final boolean impersonationEnabled;
    private static final Map<String, String> saslProperties = ImmutableMap.of(QOP, "auth", SERVER_AUTH, "true");

    @Inject
    public KerberosHiveMetastoreAuthentication(
            MetastoreKerberosConfig config,
            @ForHiveMetastore HadoopAuthentication authentication,
            HiveClientConfig hiveClientConfig,
            MetastoreClientConfig metastoreClientConfig)
    {
        this(config.getHiveMetastoreServicePrincipal(), authentication, hiveClientConfig.isHdfsWireEncryptionEnabled(), metastoreClientConfig.isMetastoreImpersonationEnabled());
    }

    public KerberosHiveMetastoreAuthentication(String hiveMetastoreServicePrincipal, HadoopAuthentication authentication, boolean hdfsWireEncryptionEnabled, boolean impersonationEnabled)
    {
        this.hiveMetastoreServicePrincipal = requireNonNull(hiveMetastoreServicePrincipal, "hiveMetastoreServicePrincipal is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
        this.hdfsWireEncryptionEnabled = hdfsWireEncryptionEnabled;
        this.impersonationEnabled = impersonationEnabled;
    }

    @Override
    public TTransport authenticateWithToken(TTransport rawTransport, String tokenString)
    {
        try {
            Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
            token.decodeFromUrlString(tokenString);

            TTransport saslTransport = new TSaslClientTransport(
                    TOKEN.getMechanismName(),
                    null,
                    null,
                    SASL_DEFAULT_REALM,
                    saslProperties,
                    new SaslClientCallbackHandler(token),
                    rawTransport);
            return new TUGIAssumingTransport(saslTransport, UserGroupInformation.getCurrentUser());
        }
        catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static class SaslClientCallbackHandler
            implements CallbackHandler
    {
        private final String userName;
        private final char[] userPassword;

        public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token)
        {
            this.userName = encodeIdentifier(token.getIdentifier());
            this.userPassword = encodePassword(token.getPassword());
        }

        @Override
        public void handle(Callback[] callbacks)
                throws UnsupportedCallbackException
        {
            for (Callback callback : callbacks) {
                if (callback instanceof RealmChoiceCallback) {
                    continue;
                }
                else if (callback instanceof NameCallback) {
                    NameCallback nameCallback = (NameCallback) callback;
                    nameCallback.setName(userName);
                }
                else if (callback instanceof PasswordCallback) {
                    PasswordCallback passwordCallback = (PasswordCallback) callback;
                    passwordCallback.setPassword(userPassword);
                }
                else if (callback instanceof RealmCallback) {
                    RealmCallback realmCallback = (RealmCallback) callback;
                    realmCallback.setText(realmCallback.getDefaultText());
                }
                else {
                    throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
                }
            }
        }

        static String encodeIdentifier(byte[] identifier)
        {
            return new String(Base64.encodeBase64(identifier));
        }

        static char[] encodePassword(byte[] password)
        {
            return new String(Base64.encodeBase64(password)).toCharArray();
        }
    }

    @Override
    public TTransport authenticate(TTransport rawTransport, String hiveMetastoreHost)
    {
        try {
            String serverPrincipal = getServerPrincipal(hiveMetastoreServicePrincipal, hiveMetastoreHost);
            String[] names = SaslRpcServer.splitKerberosName(serverPrincipal);
            checkState(names.length == 3,
                    "Kerberos principal name does NOT have the expected hostname part: %s", serverPrincipal);

            Map<String, String> saslProps = ImmutableMap.of(
                    QOP, hdfsWireEncryptionEnabled ? "auth-conf" : "auth",
                    SERVER_AUTH, "true");

            TTransport saslTransport = new TSaslClientTransport(
                    KERBEROS.getMechanismName(),
                    null,
                    names[0],
                    names[1],
                    saslProps,
                    null,
                    rawTransport);

            return new TUGIAssumingTransport(saslTransport, authentication.getUserGroupInformation());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public <R, E extends Exception> R doAs(String user, GenericExceptionAction<R, E> action)
            throws E
    {
        if (impersonationEnabled) {
            return executeActionInDoAs(createProxyUser(user), action);
        }
        return executeActionInDoAs(authentication.getUserGroupInformation(), action);
    }

    private UserGroupInformation createProxyUser(String user)
    {
        return UserGroupInformation.createProxyUser(user, authentication.getUserGroupInformation());
    }
}
