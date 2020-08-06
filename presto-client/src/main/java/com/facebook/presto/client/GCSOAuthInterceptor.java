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
package com.facebook.presto.client;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Splitter;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.client.GCSOAuthScope.DEVSTORAGE_READ_ONLY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static java.util.Objects.requireNonNull;

public class GCSOAuthInterceptor
        implements Interceptor
{
    public static final String GCS_CREDENTIALS_PATH_KEY = "hive.gcs.credentials.path";
    public static final String GCS_OAUTH_SCOPES_KEY = "hive.gcs.oauth.scopes";

    private static final String GCS_CREDENTIALS_OAUTH_TOKEN_KEY = "hive.gcs.oauth";
    private static final Splitter SCOPE_SPLITTER = Splitter.on("|");

    private final Collection<String> gcsOAuthScopeURLs;
    private final String credentialsFilePath;

    private GoogleCredentials credentials;

    public GCSOAuthInterceptor(String credentialPath, Optional<String> gcsOAuthScopesString)
    {
        this.credentialsFilePath = requireNonNull(credentialPath);
        this.gcsOAuthScopeURLs = mapScopeStringToURLs(requireNonNull(gcsOAuthScopesString).orElse(DEVSTORAGE_READ_ONLY.name()));
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        return chain.proceed(attachGCSAccessToken(chain.request()));
    }

    private Request attachGCSAccessToken(Request request)
    {
        AccessToken token = getCredentials().getAccessToken();
        return request.newBuilder()
                .addHeader(PRESTO_EXTRA_CREDENTIAL, GCS_CREDENTIALS_OAUTH_TOKEN_KEY + "=" + token.getTokenValue())
                .build();
    }

    private synchronized GoogleCredentials getCredentials()
    {
        if (credentials == null) {
            credentials = createCredentials();
        }
        try {
            credentials.refreshIfExpired();
        }
        catch (IOException e) {
            throw new ClientException("Google credential refreshing error", e);
        }
        return credentials;
    }

    private GoogleCredentials createCredentials()
    {
        try {
            return GoogleCredentials.fromStream(new FileInputStream(credentialsFilePath)).createScoped(gcsOAuthScopeURLs);
        }
        catch (IOException e) {
            throw new ClientException("Google credential loading error", e);
        }
    }

    private Collection<String> mapScopeStringToURLs(String gcsOAuthScopesString)
    {
        return StreamSupport
                .stream(SCOPE_SPLITTER.split(gcsOAuthScopesString).spliterator(), false)
                .map(GCSOAuthScope::valueOf)
                .map(scope -> scope.getScopeURL())
                .collect(Collectors.toList());
    }
}
