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
package com.facebook.presto.server.security.oauth2;

import com.facebook.airlift.units.Duration;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import jakarta.annotation.PreDestroy;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OAuth2TokenExchange
        implements OAuth2TokenHandler
{
    public static final Duration MAX_POLL_TIME = new Duration(10, SECONDS);
    private static final TokenPoll TOKEN_POLL_TIMED_OUT = TokenPoll.error("Authentication has timed out");
    private static final TokenPoll TOKEN_POLL_DROPPED = TokenPoll.error("Authentication has been finished by the client");

    private final LoadingCache<String, SettableFuture<TokenPoll>> cache;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("oauth2-token-exchange"));

    @Inject
    public OAuth2TokenExchange(OAuth2Config config)
    {
        long challengeTimeoutMillis = config.getChallengeTimeout().toMillis();
        this.cache = buildUnsafeCache(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(challengeTimeoutMillis + (MAX_POLL_TIME.toMillis() * 10), MILLISECONDS)
                        .removalListener(notification -> notification.getValue().set(TOKEN_POLL_TIMED_OUT)),
                new CacheLoader<String, SettableFuture<TokenPoll>>()
                {
                    @Override
                    public SettableFuture<TokenPoll> load(String authIdHash)
                    {
                        SettableFuture<TokenPoll> future = SettableFuture.create();
                        Future<?> timeout = executor.schedule(() -> future.set(TOKEN_POLL_TIMED_OUT), challengeTimeoutMillis, MILLISECONDS);
                        future.addListener(() -> timeout.cancel(true), executor);
                        return future;
                    }
                });
    }

    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return cacheBuilder.build(cacheLoader);
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    @Override
    public void setAccessToken(String authIdHash, String accessToken)
    {
        cache.getUnchecked(authIdHash).set(TokenPoll.token(accessToken));
    }

    @Override
    public void setTokenExchangeError(String authIdHash, String message)
    {
        cache.getUnchecked(authIdHash).set(TokenPoll.error(message));
    }

    public ListenableFuture<TokenPoll> getTokenPoll(UUID authId)
    {
        return nonCancellationPropagating(cache.getUnchecked(hashAuthId(authId)));
    }

    public void dropToken(UUID authId)
    {
        cache.getUnchecked(hashAuthId(authId)).set(TOKEN_POLL_DROPPED);
    }

    public static String hashAuthId(UUID authId)
    {
        return Hashing.sha256()
                .hashString(authId.toString(), StandardCharsets.UTF_8)
                .toString();
    }

    public static class TokenPoll
    {
        private final Optional<String> token;
        private final Optional<String> error;

        private TokenPoll(String token, String error)
        {
            this.token = Optional.ofNullable(token);
            this.error = Optional.ofNullable(error);
        }

        static TokenPoll token(String token)
        {
            requireNonNull(token, "token is null");

            return new TokenPoll(token, null);
        }

        static TokenPoll error(String error)
        {
            requireNonNull(error, "error is null");

            return new TokenPoll(null, error);
        }

        public Optional<String> getToken()
        {
            return token;
        }

        public Optional<String> getError()
        {
            return error;
        }
    }
}
