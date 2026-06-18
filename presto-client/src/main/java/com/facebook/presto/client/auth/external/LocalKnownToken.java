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

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * LocalKnownToken class keeps the token on its field
 * and it's designed to use it in fully serialized manner.
 */
@NotThreadSafe
class LocalKnownToken
        implements KnownToken
{
    private Optional<Token> knownToken = Optional.empty();

    @Override
    public Optional<Token> getToken()
    {
        return knownToken;
    }

    @Override
    public void setupToken(Supplier<Optional<Token>> tokenSource)
    {
        requireNonNull(tokenSource, "tokenSource is null");

        knownToken = tokenSource.get();
    }
}
