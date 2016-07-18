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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Splitter;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Iterator;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class UrlFunctions
{
    private static final Splitter QUERY_SPLITTER = Splitter.on('&');
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);

    private UrlFunctions() {}

    @Nullable
    @Description("extract protocol from url")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice urlExtractProtocol(@SqlType(StandardTypes.VARCHAR) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getScheme());
    }

    @Nullable
    @Description("extract host from url")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice urlExtractHost(@SqlType(StandardTypes.VARCHAR) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getHost());
    }

    @Nullable
    @Description("extract port from url")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static Long urlExtractPort(@SqlType(StandardTypes.VARCHAR) Slice url)
    {
        URI uri = parseUrl(url);
        if ((uri == null) || (uri.getPort() < 0)) {
            return null;
        }
        return (long) uri.getPort();
    }

    @Nullable
    @Description("extract part from url")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice urlExtractPath(@SqlType(StandardTypes.VARCHAR) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getPath());
    }

    @Nullable
    @Description("extract query from url")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice urlExtractQuery(@SqlType(StandardTypes.VARCHAR) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getQuery());
    }

    @Nullable
    @Description("extract fragment from url")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice urlExtractFragment(@SqlType(StandardTypes.VARCHAR) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getFragment());
    }

    @Nullable
    @Description("extract query parameter from url")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice urlExtractParameter(@SqlType(StandardTypes.VARCHAR) Slice url, @SqlType(StandardTypes.VARCHAR) Slice parameterName)
    {
        URI uri = parseUrl(url);
        if ((uri == null) || (uri.getQuery() == null)) {
            return null;
        }

        Slice query = slice(uri.getQuery());
        String parameter = parameterName.toStringUtf8();
        Iterable<String> queryArgs = QUERY_SPLITTER.split(query.toStringUtf8());

        for (String queryArg : queryArgs) {
            Iterator<String> arg = ARG_SPLITTER.split(queryArg).iterator();
            if (arg.next().equals(parameter)) {
                if (arg.hasNext()) {
                    return utf8Slice(arg.next());
                }
                // first matched key is empty
                return Slices.EMPTY_SLICE;
            }
        }

        // no key matched
        return null;
    }

    @Description("escape a string for use in URL query parameter names and values")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice urlEncode(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        Escaper escaper = UrlEscapers.urlFormParameterEscaper();
        return slice(escaper.escape(value.toStringUtf8()));
    }

    @Description("unescape a URL-encoded string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice urlDecode(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        try {
            return slice(URLDecoder.decode(value.toStringUtf8(), UTF_8.name()));
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    private static Slice slice(@Nullable String s)
    {
        return utf8Slice(nullToEmpty(s));
    }

    @Nullable
    private static URI parseUrl(Slice url)
    {
        try {
            return new URI(url.toStringUtf8());
        }
        catch (URISyntaxException e) {
            return null;
        }
    }
}
