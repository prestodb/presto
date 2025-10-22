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
package com.facebook.presto.client.auth.external;

import com.facebook.presto.client.ClientException;
import com.google.common.collect.ImmutableList;
import okhttp3.HttpUrl;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.ThrowableAssert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.client.auth.external.ExternalAuthenticator.TOKEN_URI_FIELD;
import static com.facebook.presto.client.auth.external.ExternalAuthenticator.toAuthentication;
import static com.facebook.presto.client.auth.external.MockTokenPoller.onPoll;
import static com.facebook.presto.client.auth.external.TokenPollResult.successful;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.URI.create;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestExternalAuthenticator
{
    private static final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(TestExternalAuthenticator.class.getName() + "-%d"));

    @AfterClass(alwaysRun = true)
    public void shutDownThreadPool()
    {
        executor.shutdownNow();
    }

    @Test
    public void testChallengeWithOnlyTokenServerUri()
    {
        assertThat(buildAuthentication("Bearer x_token_server=\"http://token.uri\""))
                .hasValueSatisfying(authentication -> {
                    assertThat(authentication.getRedirectUri()).isEmpty();
                    assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
                });
    }

    @Test
    public void testChallengeWithBothUri()
    {
        assertThat(buildAuthentication("Bearer x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\""))
                .hasValueSatisfying(authentication -> {
                    assertThat(authentication.getRedirectUri()).hasValue(create("http://redirect.uri"));
                    assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
                });
    }

    @Test
    public void testChallengeWithValuesWithoutQuotes()
    {
        // this is legal according to RFC 7235
        assertThat(buildAuthentication("Bearer x_redirect_server=http://redirect.uri, x_token_server=http://token.uri"))
                .hasValueSatisfying(authentication -> {
                    assertThat(authentication.getRedirectUri()).hasValue(create("http://redirect.uri"));
                    assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
                });
    }

    @Test
    public void testChallengeWithAdditionalFields()
    {
        assertThat(buildAuthentication("Bearer type=\"token\", x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\", description=\"oauth challenge\""))
                .hasValueSatisfying(authentication -> {
                    assertThat(authentication.getRedirectUri()).hasValue(create("http://redirect.uri"));
                    assertThat(authentication.getTokenUri()).isEqualTo(create("http://token.uri"));
                });
    }

    @Test
    public void testInvalidChallenges()
    {
        // no authentication parameters
        assertThat(buildAuthentication("Bearer")).isEmpty();

        // no Bearer scheme prefix
        assertThat(buildAuthentication("x_redirect_server=\"http://redirect.uri\", x_token_server=\"http://token.uri\"")).isEmpty();

        // space instead of comma
        assertThat(buildAuthentication("Bearer x_redirect_server=\"http://redirect.uri\" x_token_server=\"http://token.uri\"")).isEmpty();

        // equals sign instead of comma
        assertThat(buildAuthentication("Bearer x_redirect_server=\"http://redirect.uri\"=x_token_server=\"http://token.uri\"")).isEmpty();
    }

    @Test
    public void testChallengeWithMalformedUri()
    {
        assertThatThrownBy(() -> buildAuthentication("Bearer x_token_server=\"http://[1.1.1.1]\""))
                .isInstanceOf(ClientException.class)
                .hasMessageContaining(format("Failed to parse URI for field '%s'", TOKEN_URI_FIELD))
                .hasRootCauseInstanceOf(URISyntaxException.class);
    }

    @Test
    public void testAuthentication()
    {
        MockTokenPoller tokenPoller = new MockTokenPoller()
                .withResult(URI.create("http://token.uri"), successful(new Token("valid-token")));
        ExternalAuthenticator authenticator = new ExternalAuthenticator(uri -> {}, tokenPoller, KnownToken.local(), Duration.ofSeconds(1));

        Request authenticated = authenticator.authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\""));

        assertThat(authenticated.headers(AUTHORIZATION))
                .containsExactly("Bearer valid-token");
    }

    @Test
    public void testReAuthenticationAfterRejectingToken()
    {
        MockTokenPoller tokenPoller = new MockTokenPoller()
                .withResult(URI.create("http://token.uri"), successful(new Token("first-token")))
                .withResult(URI.create("http://token.uri"), successful(new Token("second-token")));
        ExternalAuthenticator authenticator = new ExternalAuthenticator(uri -> {}, tokenPoller, KnownToken.local(), Duration.ofSeconds(1));

        Request request = authenticator.authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\""));
        Request reAuthenticated = authenticator.authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\"", request));

        assertThat(reAuthenticated.headers(AUTHORIZATION))
                .containsExactly("Bearer second-token");
    }

    @Test(timeOut = 2000)
    public void testAuthenticationFromMultipleThreadsWithLocallyStoredToken()
    {
        MockTokenPoller tokenPoller = new MockTokenPoller()
                .withResult(URI.create("http://token.uri"), successful(new Token("valid-token-1")))
                .withResult(URI.create("http://token.uri"), successful(new Token("valid-token-2")))
                .withResult(URI.create("http://token.uri"), successful(new Token("valid-token-3")))
                .withResult(URI.create("http://token.uri"), successful(new Token("valid-token-4")));
        MockRedirectHandler redirectHandler = new MockRedirectHandler();

        List<Future<Request>> requests = times(
                4,
                () -> new ExternalAuthenticator(redirectHandler, tokenPoller, KnownToken.local(), Duration.ofSeconds(1))
                        .authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\", x_redirect_server=\"http://redirect.uri\"")))
                .map(executor::submit)
                .collect(toImmutableList());

        ConcurrentRequestAssertion assertion = new ConcurrentRequestAssertion(requests);
        assertion.requests()
                .extracting(Request::headers)
                .extracting(headers -> headers.get(AUTHORIZATION))
                .contains("Bearer valid-token-1", "Bearer valid-token-2", "Bearer valid-token-3", "Bearer valid-token-4");
        assertion.assertThatNoExceptionsHasBeenThrown();
        assertThat(redirectHandler.getRedirectionCount()).isEqualTo(4);
    }

    @Test(timeOut = 2000)
    public void testAuthenticationFromMultipleThreadsWithCachedToken()
    {
        MockTokenPoller tokenPoller = new MockTokenPoller()
                .withResult(URI.create("http://token.uri"), successful(new Token("valid-token")));
        MockRedirectHandler redirectHandler = new MockRedirectHandler()
                .sleepOnRedirect(Duration.ofSeconds(1));

        List<Future<Request>> requests = times(
                2,
                () -> new ExternalAuthenticator(redirectHandler, tokenPoller, KnownToken.memoryCached(), Duration.ofSeconds(1))
                        .authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\", x_redirect_server=\"http://redirect.uri\"")))
                .map(executor::submit)
                .collect(toImmutableList());

        ConcurrentRequestAssertion assertion = new ConcurrentRequestAssertion(requests);
        assertion.requests()
                .extracting(Request::headers)
                .extracting(headers -> headers.get(AUTHORIZATION))
                .containsOnly("Bearer valid-token");
        assertion.assertThatNoExceptionsHasBeenThrown();
        assertThat(redirectHandler.getRedirectionCount()).isEqualTo(1);
    }

    @Test(timeOut = 2000)
    public void testAuthenticationFromMultipleThreadsWithCachedTokenAfterAuthenticateFails()
    {
        MockTokenPoller tokenPoller = new MockTokenPoller()
                .withResult(URI.create("http://token.uri"), TokenPollResult.successful(new Token("first-token")))
                .withResult(URI.create("http://token.uri"), TokenPollResult.failed("external authentication error"));
        MockRedirectHandler redirectHandler = new MockRedirectHandler()
                .sleepOnRedirect(Duration.ofMillis(500));

        ExternalAuthenticator authenticator = new ExternalAuthenticator(redirectHandler, tokenPoller, KnownToken.memoryCached(), Duration.ofSeconds(1));
        Request firstRequest = authenticator.authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\", x_redirect_server=\"http://redirect.uri\""));

        List<Future<Request>> requests = times(
                4,
                () -> new ExternalAuthenticator(redirectHandler, tokenPoller, KnownToken.memoryCached(), Duration.ofSeconds(1))
                        .authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\", x_redirect_server=\"http://redirect.uri\"", firstRequest)))
                .map(executor::submit)
                .collect(toImmutableList());

        ConcurrentRequestAssertion assertion = new ConcurrentRequestAssertion(requests);
        assertion.firstException().hasMessage("external authentication error")
                .isInstanceOf(ClientException.class);

        assertThat(redirectHandler.getRedirectionCount()).isEqualTo(2);
    }

    @Test(timeOut = 2000)
    public void testAuthenticationFromMultipleThreadsWithCachedTokenAfterAuthenticateTimesOut()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler()
                .sleepOnRedirect(Duration.ofSeconds(1));

        List<Future<Request>> requests = times(
                2,
                () -> new ExternalAuthenticator(redirectHandler, onPoll(TokenPollResult::pending), KnownToken.memoryCached(), Duration.ofMillis(1))
                        .authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\", x_redirect_server=\"http://redirect.uri\"")))
                .map(executor::submit)
                .collect(toImmutableList());

        ConcurrentRequestAssertion assertion = new ConcurrentRequestAssertion(requests);
        assertion.requests()
                .containsExactly(null, null);
        assertion.assertThatNoExceptionsHasBeenThrown();
        assertThat(redirectHandler.getRedirectionCount()).isEqualTo(1);
    }

    @Test(timeOut = 2000)
    public void testAuthenticationFromMultipleThreadsWithCachedTokenAfterAuthenticateIsInterrupted()
            throws Exception
    {
        ExecutorService interruptableThreadPool = newCachedThreadPool(daemonThreadsNamed(this.getClass().getName() + "-interruptable-%d"));
        MockRedirectHandler redirectHandler = new MockRedirectHandler()
                .sleepOnRedirect(Duration.ofMinutes(1));

        ExternalAuthenticator authenticator = new ExternalAuthenticator(redirectHandler, onPoll(TokenPollResult::pending), KnownToken.memoryCached(), Duration.ofMillis(1));
        Future<Request> interruptedAuthentication = interruptableThreadPool.submit(
                () -> authenticator.authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\", x_redirect_server=\"http://redirect.uri\"")));
        Thread.sleep(100); //It's here to make sure that authentication will start before the other threads.
        List<Future<Request>> requests = times(
                2,
                () -> new ExternalAuthenticator(redirectHandler, onPoll(TokenPollResult::pending), KnownToken.memoryCached(), Duration.ofMillis(1))
                        .authenticate(null, getUnauthorizedResponse("Bearer x_token_server=\"http://token.uri\", x_redirect_server=\"http://redirect.uri\"")))
                .map(executor::submit)
                .collect(toImmutableList());

        Thread.sleep(100);
        interruptableThreadPool.shutdownNow();

        ConcurrentRequestAssertion assertion = new ConcurrentRequestAssertion(ImmutableList.<Future<Request>>builder()
                .addAll(requests)
                .add(interruptedAuthentication)
                .build());
        assertion.requests().containsExactly(null, null);
        assertion.firstException().hasRootCauseInstanceOf(InterruptedException.class);

        assertThat(redirectHandler.getRedirectionCount()).isEqualTo(1);
    }

    private static Stream<Callable<Request>> times(int times, Callable<Request> request)
    {
        return Stream.generate(() -> request)
                .limit(times);
    }

    private static Optional<ExternalAuthentication> buildAuthentication(String challengeHeader)
    {
        return toAuthentication(getUnauthorizedResponse(challengeHeader));
    }

    private static Response getUnauthorizedResponse(String challengeHeader)
    {
        return getUnauthorizedResponse(challengeHeader,
                new Request.Builder()
                        .url(HttpUrl.get("http://example.com"))
                        .build());
    }

    private static Response getUnauthorizedResponse(String challengeHeader, Request request)
    {
        return new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(HTTP_UNAUTHORIZED)
                .message("Unauthorized")
                .header(WWW_AUTHENTICATE, challengeHeader)
                .build();
    }

    static class ConcurrentRequestAssertion
    {
        private final List<Throwable> exceptions = new ArrayList<>();
        private final List<Request> requests = new ArrayList<>();

        public ConcurrentRequestAssertion(List<Future<Request>> requests)
        {
            for (Future<Request> request : requests) {
                try {
                    this.requests.add(request.get());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                catch (CancellationException ex) {
                    exceptions.add(ex);
                }
                catch (ExecutionException ex) {
                    checkState(ex.getCause() != null, "Missing cause on ExecutionException " + ex.getMessage());

                    exceptions.add(ex.getCause());
                }
            }
        }

        ThrowableAssert firstException()
        {
            return exceptions.stream()
                    .findFirst()
                    .map(ThrowableAssert::new)
                    .orElseGet(() -> new ThrowableAssert(() -> null));
        }

        void assertThatNoExceptionsHasBeenThrown()
        {
            if (!exceptions.isEmpty()) {
                Throwable firstException = exceptions.get(0);
                AssertionError assertionError = new AssertionError("Expected no exceptions, but some exceptions has been thrown", firstException);
                for (int i = 1; i < exceptions.size(); i++) {
                    assertionError.addSuppressed(exceptions.get(i));
                }
                throw assertionError;
            }
        }

        ListAssert<Request> requests()
        {
            return assertThat(requests);
        }
    }
}
