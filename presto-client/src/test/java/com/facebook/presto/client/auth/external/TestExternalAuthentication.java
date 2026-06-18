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
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.net.URI.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestExternalAuthentication
{
    private static final String AUTH_TOKEN = "authToken";
    private static final URI REDIRECT_URI = create("https://redirect.uri");
    private static final URI TOKEN_URI = create("https://token.uri");
    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    @Test
    public void testObtainTokenWhenTokenAlreadyExists()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();

        MockTokenPoller poller = new MockTokenPoller()
                .withResult(TOKEN_URI, TokenPollResult.successful(new Token(AUTH_TOKEN)));

        Optional<Token> token = new ExternalAuthentication(TOKEN_URI, Optional.of(REDIRECT_URI))
                .obtainToken(TIMEOUT, redirectHandler, poller);

        assertThat(redirectHandler.redirectedTo()).isEqualTo(REDIRECT_URI);
        assertThat(token).map(Token::token).hasValue(AUTH_TOKEN);
        assertThat(poller.tokenReceivedUri()).isEqualTo(TOKEN_URI);
    }

    @Test
    public void testObtainTokenWhenTokenIsReadyAtSecondAttempt()
    {
        RedirectHandler redirectHandler = new MockRedirectHandler();

        URI nextTokenUri = TOKEN_URI.resolve("/next");
        MockTokenPoller poller = new MockTokenPoller()
                .withResult(TOKEN_URI, TokenPollResult.pending(nextTokenUri))
                .withResult(nextTokenUri, TokenPollResult.successful(new Token(AUTH_TOKEN)));

        Optional<Token> token = new ExternalAuthentication(TOKEN_URI, Optional.of(REDIRECT_URI))
                .obtainToken(TIMEOUT, redirectHandler, poller);

        assertThat(token).map(Token::token).hasValue(AUTH_TOKEN);
        assertThat(poller.tokenReceivedUri()).isEqualTo(nextTokenUri);
    }

    @Test
    public void testObtainTokenWhenTokenIsNeverAvailable()
    {
        RedirectHandler redirectHandler = new MockRedirectHandler();

        TokenPoller poller = MockTokenPoller.onPoll(tokenUri -> {
            sleepUninterruptibly(20, TimeUnit.MILLISECONDS);
            return TokenPollResult.pending(TOKEN_URI);
        });

        Optional<Token> token = new ExternalAuthentication(TOKEN_URI, Optional.of(REDIRECT_URI))
                .obtainToken(TIMEOUT, redirectHandler, poller);

        assertThat(token).isEmpty();
    }

    @Test
    public void testObtainTokenWhenPollingFails()
    {
        RedirectHandler redirectHandler = new MockRedirectHandler();

        TokenPoller poller = new MockTokenPoller()
                .withResult(TOKEN_URI, TokenPollResult.failed("error"));

        assertThatThrownBy(() -> new ExternalAuthentication(TOKEN_URI, Optional.of(REDIRECT_URI))
                .obtainToken(TIMEOUT, redirectHandler, poller))
                .isInstanceOf(ClientException.class)
                .hasMessage("error");
    }

    @Test
    public void testObtainTokenWhenPollingFailsWithException()
    {
        RedirectHandler redirectHandler = new MockRedirectHandler();

        TokenPoller poller = MockTokenPoller.onPoll(uri -> {
            throw new UncheckedIOException(new IOException("polling error"));
        });

        assertThatThrownBy(() -> new ExternalAuthentication(TOKEN_URI, Optional.of(REDIRECT_URI))
                .obtainToken(TIMEOUT, redirectHandler, poller))
                .isInstanceOf(UncheckedIOException.class)
                .hasRootCauseInstanceOf(IOException.class);
    }

    @Test
    public void testObtainTokenWhenNoRedirectUriHasBeenProvided()
    {
        MockRedirectHandler redirectHandler = new MockRedirectHandler();

        TokenPoller poller = new MockTokenPoller()
                .withResult(TOKEN_URI, TokenPollResult.successful(new Token(AUTH_TOKEN)));

        Optional<Token> token = new ExternalAuthentication(TOKEN_URI, Optional.empty())
                .obtainToken(TIMEOUT, redirectHandler, poller);

        assertThat(redirectHandler.redirectedTo()).isNull();
        assertThat(token).map(Token::token).hasValue(AUTH_TOKEN);
    }
}
