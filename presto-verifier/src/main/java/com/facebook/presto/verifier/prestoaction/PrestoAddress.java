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
package com.facebook.presto.verifier.prestoaction;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public interface PrestoAddress
{
    String getHost();

    int getJdbcPort();

    Optional<Integer> getHttpPort();

    Map<String, String> getJdbcUrlParameters();

    default String getJdbcUrl()
    {
        String query = getJdbcUrlParameters().entrySet().stream()
                .map(entry -> format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(joining("&"));
        return format("jdbc:presto://%s:%s?%s", getHost(), getJdbcPort(), query);
    }

    default URI getHttpUri(String path)
    {
        checkState(getHttpPort().isPresent(), "httpPort is not present");
        return URI.create(format("http://%s:%s", getHost(), getHttpPort().get())).resolve(path);
    }
}
