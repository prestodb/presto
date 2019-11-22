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
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.SocketChannelSocketFactory;
import com.facebook.presto.client.StatementClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.client.ClientSession.stripTransactionId;
import static com.facebook.presto.client.OkHttpUtil.basicAuth;
import static com.facebook.presto.client.OkHttpUtil.setupCookieJar;
import static com.facebook.presto.client.OkHttpUtil.setupHttpProxy;
import static com.facebook.presto.client.OkHttpUtil.setupKerberos;
import static com.facebook.presto.client.OkHttpUtil.setupSocksProxy;
import static com.facebook.presto.client.OkHttpUtil.setupSsl;
import static com.facebook.presto.client.OkHttpUtil.setupTimeouts;
import static com.facebook.presto.client.OkHttpUtil.tokenAuth;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryRunner
        implements Closeable
{
    private final AtomicReference<ClientSession> session;
    private final boolean debug;
    private final OkHttpClient httpClient;
    private final Consumer<OkHttpClient.Builder> sslSetup;

    public QueryRunner(
            ClientSession session,
            boolean debug,
            Optional<HostAndPort> socksProxy,
            Optional<HostAndPort> httpProxy,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword,
            Optional<String> accessToken,
            Optional<String> user,
            Optional<String> password,
            Optional<String> kerberosPrincipal,
            Optional<String> kerberosRemoteServiceName,
            Optional<String> kerberosConfigPath,
            Optional<String> kerberosKeytabPath,
            Optional<String> kerberosCredentialCachePath,
            boolean kerberosUseCanonicalHostname,
            boolean useOkta)
    {
        this.session = new AtomicReference<>(requireNonNull(session, "session is null"));
        this.debug = debug;

        this.sslSetup = builder -> setupSsl(builder, keystorePath, keystorePassword, truststorePath, truststorePassword);

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        builder.socketFactory(new SocketChannelSocketFactory());

        setupTimeouts(builder, 30, SECONDS);
        setupCookieJar(builder);
        setupSocksProxy(builder, socksProxy);
        setupHttpProxy(builder, httpProxy);
        setupBasicAuth(builder, session, user, password);
        setupTokenAuth(builder, session, accessToken);
        setupOktaAuth(builder, session, useOkta);

        if (kerberosRemoteServiceName.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using Kerberos requires HTTPS to be enabled");
            setupKerberos(
                    builder,
                    kerberosRemoteServiceName.get(),
                    kerberosUseCanonicalHostname,
                    kerberosPrincipal,
                    kerberosConfigPath.map(File::new),
                    kerberosKeytabPath.map(File::new),
                    kerberosCredentialCachePath.map(File::new));
        }

        this.httpClient = builder.build();
    }

    public ClientSession getSession()
    {
        return session.get();
    }

    public void setSession(ClientSession session)
    {
        this.session.set(requireNonNull(session, "session is null"));
    }

    public boolean isDebug()
    {
        return debug;
    }

    public Query startQuery(String query)
    {
        return new Query(startInternalQuery(session.get(), query), debug);
    }

    public StatementClient startInternalQuery(String query)
    {
        return startInternalQuery(stripTransactionId(session.get()), query);
    }

    private StatementClient startInternalQuery(ClientSession session, String query)
    {
        OkHttpClient.Builder builder = httpClient.newBuilder();
        sslSetup.accept(builder);
        OkHttpClient client = builder.build();

        return newStatementClient(client, session, query);
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private static void setupBasicAuth(
            OkHttpClient.Builder clientBuilder,
            ClientSession session,
            Optional<String> user,
            Optional<String> password)
    {
        if (user.isPresent() && password.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using username/password requires HTTPS to be enabled");
            clientBuilder.addInterceptor(basicAuth(user.get(), password.get()));
        }
    }

    private static void setupTokenAuth(
            OkHttpClient.Builder clientBuilder,
            ClientSession session,
            Optional<String> accessToken)
    {
        if (accessToken.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using an access token requires HTTPS to be enabled");
            clientBuilder.addInterceptor(tokenAuth(accessToken.get()));
        }
    }

    private static void setupOktaAuth(
            OkHttpClient.Builder clientBuilder,
            ClientSession session,
            boolean useOkta) {
        if (useOkta) {
            System.out.println("Asking for okta authentication");
            User user = new User();
            Server server = new Server(5000);
            server.setHandler(new AuthenticationHandler(server, user));

            try {
                server.start();

                // Open browser
                Desktop desktop = java.awt.Desktop.getDesktop();
                URI loginUrl = new URI("http://localhost:5000/login");
                desktop.browse(loginUrl);

                server.join();
                System.out.println("Server exited");
                System.out.println("Access Token: " + user.getAccessToken());
                // TODO: This should set access token
//                setupTokenAuth(clientBuilder, session, Optional.ofNullable(user.getAccessToken()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

class User {
    String email;
    String accessToken;

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
}


class AuthenticationHandler extends AbstractHandler {

    String REDIRECT_URI = "http://localhost:5000/authorization-code/callback";

    String AUDIENCE = "api://default";
    String STATE = "SomeState";

    String CLIENT_ID = "0oa1vnlzvlKQMmYzo357";
    String CLIENT_SECRET = "ql_yA0vo52cm1qOPp5MyUEU8lQdWd-ezIqTZu22N";
    String BASE_URL = "https://dev-778936.okta.com";
    String ISSUER = BASE_URL + "/oauth2/default";
    String TOKEN_ENDPOINT = BASE_URL + "/oauth2/default/v1/token";
    String LOGIN_ENDPOINT = BASE_URL + "/oauth2/default/v1/authorize";
    String LOGIN_URL = LOGIN_ENDPOINT + "?"
            + "client_id=" + CLIENT_ID + "&"
            + "redirect_uri=" + REDIRECT_URI + "&"
            + "response_type=code&"
            + "scope=openid&"
            + "state=" + STATE;

    private Server server;
    private User user;

    AuthenticationHandler(Server server, User user) {
        this.server = server;
        this.user = user;
    }

    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException, ServletException {
        baseRequest.setHandled(true);

        if (target.equalsIgnoreCase("/hello")) {
            handleHello(baseRequest, response);
        } else if (target.equalsIgnoreCase("/authorization-code/callback")) {
            handleCallback(request, response);
        } else if (target.equalsIgnoreCase("/login")) {
            handleLogin(response);
        }
    }

    private void handleHello(Request baseRequest, HttpServletResponse response) throws IOException {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        response.getWriter().println("<h1>Hello World</h1>");
    }

    private void handleCallback(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // Read the code
        response.getWriter().println("<p>Handling callback</p>");
        String code = request.getParameter("code");
        response.getWriter().println("<p>Code: " + code + "</p>");

        // Now get the auth token
        RequestBody formBody = new FormBody.Builder()
                .add("grant_type", "authorization_code")
                .add("code", code)
                .add("redirect_uri", REDIRECT_URI)
                .add("client_id", CLIENT_ID)
                .add("client_secret", CLIENT_SECRET)
                .build();

        okhttp3.Request accessTokenRequest = new okhttp3.Request.Builder()
                .url(TOKEN_ENDPOINT)
                .addHeader("User-Agent", "OkHttp Bot")
                .post(formBody)
                .build();

        OkHttpClient okHttpClient = new OkHttpClient();
        Response accessTokenResponse = okHttpClient.newCall(accessTokenRequest).execute();
        String accessTokenResponseBody = accessTokenResponse.body().string();
        response.getWriter().println("<p>Auth Response: " + accessTokenResponseBody + "</p>");

        // Parse the token
        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsedJson = mapper.readTree(accessTokenResponseBody);
        String accessToken = parsedJson.get("access_token").toString();
        response.getWriter().println("<p>Access Token: " + accessToken + "</p>");

        response.getWriter().println("<a onclick='window.close();'>Close Window</a>");

//        response.getWriter().println("<p>Stopping the server</p>");
        response.flushBuffer(); // Necessary to show output on the screen

        // Set the user
        user.setAccessToken(accessToken);

        // Stop the server.
        try {
            new Thread(() -> {
                try {
                    System.out.println("Shutting down Jetty...");
                    server.stop();
                    System.out.println("Jetty has stopped.");
                } catch (Exception ex) {
                    System.out.println("Error when stopping Jetty: " + ex.getMessage());
                }
            }).start();

        } catch (Exception e) {
            System.out.println("Cannot stop server");
            e.printStackTrace();
        }
    }

    private void handleLogin(HttpServletResponse response) throws IOException {
        response.sendRedirect(LOGIN_URL);
    }
}
