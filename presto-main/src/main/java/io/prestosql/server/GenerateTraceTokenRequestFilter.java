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
package io.prestosql.server;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;

import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.airlift.http.client.TraceTokenRequestFilter.TRACETOKEN_HEADER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class GenerateTraceTokenRequestFilter
        implements HttpRequestFilter
{
    private final String prefix = randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
    private final AtomicLong sequence = new AtomicLong();

    @Override
    public Request filterRequest(Request request)
    {
        requireNonNull(request, "request is null");
        String newToken = createToken(sequence.getAndIncrement());
        return fromRequest(request)
                .setHeader(TRACETOKEN_HEADER, newToken)
                .build();
    }

    private String createToken(long value)
    {
        return prefix + format("%010x", value);
    }

    @VisibleForTesting
    String getLastToken()
    {
        long value = sequence.get();
        return createToken(value - 1);
    }
}
