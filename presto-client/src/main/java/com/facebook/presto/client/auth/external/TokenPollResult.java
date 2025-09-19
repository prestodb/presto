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

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TokenPollResult
{
    private enum State
    {
        PENDING, SUCCESSFUL, FAILED
    }

    private final State state;
    private final Optional<String> errorMessage;
    private final Optional<URI> nextTokenUri;
    private final Optional<Token> token;

    public static TokenPollResult failed(String error)
    {
        return new TokenPollResult(State.FAILED, null, error, null);
    }

    public static TokenPollResult pending(URI uri)
    {
        return new TokenPollResult(State.PENDING, null, null, uri);
    }

    public static TokenPollResult successful(Token token)
    {
        return new TokenPollResult(State.SUCCESSFUL, token, null, null);
    }

    private TokenPollResult(State state, Token token, String error, URI nextTokenUri)
    {
        this.state = requireNonNull(state, "state is null");
        this.token = Optional.ofNullable(token);
        this.errorMessage = Optional.ofNullable(error);
        this.nextTokenUri = Optional.ofNullable(nextTokenUri);
    }

    public boolean isFailed()
    {
        return state == State.FAILED;
    }

    public boolean isPending()
    {
        return state == State.PENDING;
    }

    public Token getToken()
    {
        return token.orElseThrow(() -> new IllegalStateException("state is " + state));
    }

    public String getError()
    {
        return errorMessage.orElseThrow(() -> new IllegalStateException("state is " + state));
    }

    public URI getNextTokenUri()
    {
        return nextTokenUri.orElseThrow(() -> new IllegalStateException("state is " + state));
    }
}
