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

import com.facebook.airlift.log.Logger;
import jakarta.inject.Inject;
import org.apache.hadoop.hive.metastore.MetaStorePlainSaslHelper;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CustomHiveMetastoreAuthentication
        implements HiveMetastoreAuthentication
{
    private static final Logger log = Logger.get(CustomHiveMetastoreAuthentication.class);
    private final String hiveMetastoreClientPlainUsername;
    private final String hiveMetastoreClientPlainToken;

    @Inject
    public CustomHiveMetastoreAuthentication(MetastoreCustomAuthConfig config)
    {
        this(config.getHiveMetastoreClientPlainUsername(), config.getHiveMetastoreClientPlainToken());
    }

    public CustomHiveMetastoreAuthentication(String hiveMetastoreClientPlainUsername, String hiveMetastoreClientPlainToken)
    {
        this.hiveMetastoreClientPlainToken = requireNonNull(hiveMetastoreClientPlainToken, "hiveMetastoreServiceToken is null");
        this.hiveMetastoreClientPlainUsername = requireNonNull(hiveMetastoreClientPlainUsername, "hiveMetastoreServiceUsername is null");
    }

    @Override
    public TTransport authenticate(TTransport rawTransport, String hiveMetastoreHost, Optional<String> tokenString)
    {
        return createAuthBinaryTransport(rawTransport, this.hiveMetastoreClientPlainUsername, this.hiveMetastoreClientPlainToken);
    }

    private TTransport createAuthBinaryTransport(TTransport underlyingTransport, String userName, String token)
    {
        TTransport transport;
        try {
            // Overlay the SASL transport on top of the base socket transport (SSL or non-SSL)
            transport = new TSaslClientTransport("PLAIN", null, null, null, new HashMap<String, String>(),
                    new MetaStorePlainSaslHelper.PlainCallbackHandler(userName, token), underlyingTransport);
            log.debug("HMS custom authentication send with username and password");
        }
        catch (IOException | TTransportException ex) {
            throw new UncheckedIOException((IOException) ex);
        }
        return transport;
    }
}
