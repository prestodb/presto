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

import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;

public final class MockTokenPoller
        implements TokenPoller
{
    private final Map<URI, BlockingDeque<TokenPollResult>> results = new ConcurrentHashMap<>();
    private URI tokenReceivedUri;

    public static TokenPoller onPoll(Function<URI, TokenPollResult> pollingStrategy)
    {
        return new TokenPoller()
        {
            @Override
            public TokenPollResult pollForToken(URI tokenUri, Duration timeout)
            {
                return pollingStrategy.apply(tokenUri);
            }

            @Override
            public void tokenReceived(URI tokenUri)
            {
            }
        };
    }

    public MockTokenPoller withResult(URI tokenUri, TokenPollResult result)
    {
        results.compute(tokenUri, (uri, queue) -> {
            if (queue == null) {
                return new LinkedBlockingDeque<>(ImmutableList.of(result));
            }
            queue.add(result);
            return queue;
        });
        return this;
    }

    @Override
    public TokenPollResult pollForToken(URI tokenUri, Duration ignored)
    {
        BlockingDeque<TokenPollResult> queue = results.get(tokenUri);
        if (queue == null) {
            throw new IllegalArgumentException("Unknown token URI: " + tokenUri);
        }
        return queue.remove();
    }

    @Override
    public void tokenReceived(URI tokenUri)
    {
        this.tokenReceivedUri = tokenUri;
    }

    public URI tokenReceivedUri()
    {
        return tokenReceivedUri;
    }
}
