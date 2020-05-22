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
import okhttp3.Authenticator;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import javax.annotation.Nullable;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_EXTRA_CREDENTIAL;
import static java.util.Objects.requireNonNull;

public class GCSOAuthHandler
        implements Interceptor, Authenticator
{

    public static final String GCS_CREDENTIALS_PATH_KEY = "hive.gcs.credentials.path";
    private static final String GCS_CREDENTIALS_OAUTH_TOKEN_KEY = "hive.gcs.oauth";
    private final ClientSession session;

    private String credentialsFilePath;

    private GoogleCredentials credentials;

    public GCSOAuthHandler(ClientSession session)
    {
        this.session = requireNonNull(session);
        this.credentialsFilePath = session.getExtraCredentials().get(GCS_CREDENTIALS_PATH_KEY);
    }

    @Nullable
    @Override
    public Request authenticate(Route route, Response response)
            throws IOException
    {
        return attachGCSAccessToken(response.request());
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        try {
            return chain.proceed(attachGCSAccessToken(chain.request()));
        }
        catch (ClientException ignored) {
            return chain.proceed(chain.request());
        }
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
            return GoogleCredentials.fromStream(new FileInputStream(credentialsFilePath)).createScoped(Collections.singleton("https://www.googleapis.com/auth/devstorage.read_only"));
        }
        catch (IOException e) {
            throw new ClientException("Google credential loading error", e);
        }
    }
}
