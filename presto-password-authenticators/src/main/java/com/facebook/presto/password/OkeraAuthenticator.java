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
package com.facebook.presto.password;

import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.BasicPrincipal;
import com.facebook.presto.spi.security.PasswordAuthenticator;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Base64;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * User/password authenticator that supports the various ways ODAS can be authenticated.
 * This class does this by asking the REST server to authenticate (identical to how the
 * UI does it).
 */
public class OkeraAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger LOG = Logger.get(OkeraAuthenticator.class);
    private static final int MIN_TOKEN_LEN = 40;

    private final boolean authenticationEnabled;
    private static boolean restServerSslConfigured;

    private final LoadingCache<LdapAuthenticator.Credentials, Principal> authenticationCache;
    private URL url;
    private long timeoutMs;

    private static boolean envVarSet(String name)
    {
        String v = System.getenv(name);
        return v != null && v.length() > 0;
    }

    @Inject
    public OkeraAuthenticator(OkeraConfig serverConfig)
    {
        // Authentication is enabled on this system if either of these are set.
        if (envVarSet("SYSTEM_TOKEN") || envVarSet("KERBEROS_KEYTAB_FILE")) {
            LOG.info("Configuring CDAS_REST_SERVER for authentication.");

            if (!envVarSet("CDAS_REST_SERVER_SERVICE_HOST") && !envVarSet("CEREBRO_REST_FQDN")) {
                throw new IllegalStateException("Expecting CDAS_REST_SERVER_SERVICE_HOST or CEREBRO_REST_FQDN to be set in the environment.");
            }

            String host;
            String port;
            if (envVarSet("CEREBRO_REST_FQDN")) {
                // Configured it explicitly
                host = System.getenv("CEREBRO_REST_FQDN");
                if (host == null) {
                    host = System.getenv("CDAS_REST_SERVER_SERVICE_HOST");
                }
                port = System.getenv("CDAS_REST_SERVER_SERVICE_PORT");
            }
            else {
                // Configured via k8s
                host = System.getenv("CDAS_REST_SERVER_SERVICE_HOST");
                port = System.getenv("CDAS_REST_SERVER_SERVICE_PORT");
            }

            authenticationEnabled = true;
            try {
                String urlString = "";
                if (envVarSet("SSL_KEY_FILE")) {
                    configureRestServerSsl();
                    urlString = "https://";
                }
                else {
                    urlString = "http://";
                }
                urlString += host + ":" + port;
                url = new URL(urlString + "/api/get-user");
                LOG.info("Configured backing authentication service: " + url);
            }
            catch (KeyManagementException | NoSuchAlgorithmException | MalformedURLException e) {
                throw new UncheckedExecutionException("Could not configure REST server connection.", e);
            }
        }
        else {
            LOG.warn("Authentication is enabled on this cluster.");
            authenticationEnabled = false;
        }

        this.authenticationCache = CacheBuilder.newBuilder()
            .expireAfterWrite(serverConfig.getCacheTtl().toMillis(), MILLISECONDS)
            .build(CacheLoader.from(this::authenticate));
        this.timeoutMs = serverConfig.getAuthTimeout().toMillis();
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        try {
            return authenticationCache.getUnchecked(new LdapAuthenticator.Credentials(user, password));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), AccessDeniedException.class);
            throw e;
        }
    }

    private Principal authenticate(LdapAuthenticator.Credentials credentials)
    {
        return authenticate(credentials.getUser(), credentials.getPassword());
    }

    private Principal authenticate(String user, String password)
    {
        if (!authenticationEnabled) {
            if (!user.equals(password)) {
                LOG.warn("Authentication error for user [%s]", user);
                throw new AccessDeniedException("Authentication error for user: " + user);
            }
            return new BasicPrincipal(user);
        }
        try {
            return new BasicPrincipal(authenticateRestServer(user, password));
        }
        catch (IOException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    private String authenticateRestServer(String user, String passwordOrToken) throws IOException
    {
        user = user.trim();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        if (timeoutMs > 0) {
            conn.setConnectTimeout((int) timeoutMs);
            conn.setReadTimeout((int) timeoutMs);
        }
        conn.setRequestProperty("Accept", "application/json");

        // Determine if this user specified a password or token
        if (isLikelyToken(passwordOrToken)) {
            conn.setRequestProperty("Authorization", "Bearer " + passwordOrToken);
        }
        else {
            final String userpass = user + ":" + passwordOrToken;
            final String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
            conn.setRequestProperty("Authorization", basicAuth);
        }

        if (conn.getResponseCode() != 200) {
            LOG.warn("Failed to authenticate user: " + user);
            String error = readStreamToString(conn.getErrorStream());
            if (!error.isEmpty()) {
                // In this case, the server returned an error. Output that more prominently,
                // chances are it is more helpful than the generic HTTP response fields.
                if (conn.getResponseCode() >= 400 && conn.getResponseCode() < 500) {
                    // Don't retry 40* error codes. The server explicitly failed this, not
                    // likely transient and just makes things more noisy/take longer to fail.
                    if (conn.getResponseCode() == 401 || conn.getResponseCode() == 403) {
                        throw new AccessDeniedException(String.format(
                            "Failed to authenticate user. Server returned:\n%s\n" +
                            "Details: HTTP error code: %d. Response message: %s URL: %s Header: %s",
                            error, conn.getResponseCode(), conn.getResponseMessage(),
                            conn.getURL(), conn.getHeaderFields()));
                    }
                    throw new IOException(String.format(
                        "Failed to authenticate user. Server returned:\n%s\n" +
                        "Details: HTTP error code: %d. Response message: %s URL: %s Header: %s",
                        error, conn.getResponseCode(), conn.getResponseMessage(),
                        conn.getURL(), conn.getHeaderFields()));
                }
            }
            else {
                throw new IOException(String.format(
                    "Failed to authenticate user. Server returned:\n%s\n" +
                    "Details: HTTP error code: %d. Response message: %s URL: %s Header: %s",
                    error, conn.getResponseCode(), conn.getResponseMessage(),
                    conn.getURL(), conn.getHeaderFields()));
            }
        }

        // We don't use a json library to extract the username because of dependency management.
        // Loading the jackson library in this module is non-trivial.
        // We are parsing the json for "user": <thing we want>
        String json = readStreamToString(conn.getInputStream());
        final String userKey = "\"user\": \"";
        int idx = json.indexOf(userKey);
        if (idx == -1) {
            throw new IOException("Server returned invalid response. Missing 'user'");
        }
        String sub = json.substring(idx + userKey.length());
        String authenticatedUser = sub.substring(0, sub.indexOf("\"")).trim();

        // Verify that the authenticated user matches what was specified. In the case of JWT tokens
        // in particular, they can be mismatched. While we could handle this (have the token one
        // superceded, this confuses presto - the UI shows the specified user for example).
        if (!user.equals(authenticatedUser)) {
            LOG.warn("Authentication error for user [%s]. Specified username did not match.", user);
            throw new AccessDeniedException("Authentication error for user: " + user + ". If using token based authentication, username must match user in token.");
        }
        return authenticatedUser;
    }

    private static synchronized void configureRestServerSsl() throws KeyManagementException, NoSuchAlgorithmException
    {
        if (restServerSslConfigured) {
            return;
        }

        // Configure the all-trusting cert and ignore host mismatch. This is used for internal
        // (within cluster) communication only.
        TrustManager[] trustAllCerts = new TrustManager[] {
            new X509TrustManager() {
                @Override
                public java.security.cert.X509Certificate[] getAcceptedIssuers()
                {
                    return new X509Certificate[0];
                }

                @Override
                public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType)
                {
                }

                @Override
                public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType)
                {
                }
            }
        };

        HostnameVerifier hv = new HostnameVerifier() {
            @Override
            public boolean verify(String urlHostName, SSLSession session)
            {
                return true;
            }
        };

        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        HttpsURLConnection.setDefaultHostnameVerifier(hv);

        restServerSslConfigured = true;
    }

    private static boolean isLikelyToken(String s)
    {
        String[] parts = s.split("\\.");
        if (parts.length != 2 && parts.length != 3) {
            return false;
        }
        return s.length() > MIN_TOKEN_LEN;
    }

    private static String readStreamToString(InputStream stream) throws IOException
    {
        if (stream == null) {
            return "";
        }

        StringBuilder buf = new StringBuilder();
        BufferedReader reader = null;
        InputStreamReader isr = null;

        try {
            isr = new InputStreamReader(stream);
            reader = new BufferedReader(isr);
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (buf.length() != 0) {
                    buf.append("\n");
                }
                buf.append(line);
            }
        }
        finally {
            if (reader != null) {
                reader.close();
            }
            if (isr != null) {
                isr.close();
            }
        }
        return buf.toString();
    }
}
