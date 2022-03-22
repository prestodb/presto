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
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
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
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

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
    private static final Map<String, String> SASL_PROPERTIES = ImmutableMap.of(QOP, "auth", SERVER_AUTH, "true");
    private final String hiveMetastoreServicePrincipal;
    private final HadoopAuthentication authentication;
    private final boolean hdfsWireEncryptionEnabled;

    @Inject
    public KerberosHiveMetastoreAuthentication(
            MetastoreKerberosConfig config,
            @ForHiveMetastore HadoopAuthentication authentication,
            HiveClientConfig hiveClientConfig)
    {
        this(config.getHiveMetastoreServicePrincipal(), authentication, hiveClientConfig.isHdfsWireEncryptionEnabled());
    }

    public KerberosHiveMetastoreAuthentication(String hiveMetastoreServicePrincipal, HadoopAuthentication authentication, boolean hdfsWireEncryptionEnabled)
    {
        this.hiveMetastoreServicePrincipal = requireNonNull(hiveMetastoreServicePrincipal, "hiveMetastoreServicePrincipal is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
        this.hdfsWireEncryptionEnabled = hdfsWireEncryptionEnabled;
    }

    @Override
    public TTransport authenticate(TTransport rawTransport, String hiveMetastoreHost, Optional<String> tokenString)
    {
        return tokenString.map(s -> authenticateWithToken(rawTransport, s)).orElseGet(() -> authenticateWithHost(rawTransport, hiveMetastoreHost));
    }

    private TTransport authenticateWithToken(TTransport rawTransport, String tokenString)
    {
        try {
            Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
            token.decodeFromUrlString(tokenString);

            TTransport saslTransport = new TSaslClientTransport(
                    TOKEN.getMechanismName(),
                    null,
                    null,
                    SASL_DEFAULT_REALM,
                    SASL_PROPERTIES,
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

        private static String encodeIdentifier(byte[] identifier)
        {
            return Base64.getEncoder().encodeToString(identifier);
        }

        private static char[] encodePassword(byte[] password)
        {
            return Base64.getEncoder().encodeToString(password).toCharArray();
        }
    }

    private TTransport authenticateWithHost(TTransport rawTransport, String hiveMetastoreHost)
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
}
