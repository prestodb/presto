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
package io.prestosql.operator.scalar;

import com.google.common.base.Splitter;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.type.Constraint;

import javax.annotation.Nullable;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Iterator;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class UrlFunctions
{
    private static final Splitter QUERY_SPLITTER = Splitter.on('&');
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);

    private UrlFunctions() {}

    @SqlNullable
    @Description("extract protocol from url")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlExtractProtocol(@SqlType("varchar(x)") Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getScheme());
    }

    @SqlNullable
    @Description("extract host from url")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlExtractHost(@SqlType("varchar(x)") Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getHost());
    }

    @SqlNullable
    @Description("extract port from url")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static Long urlExtractPort(@SqlType("varchar(x)") Slice url)
    {
        URI uri = parseUrl(url);
        if ((uri == null) || (uri.getPort() < 0)) {
            return null;
        }
        return (long) uri.getPort();
    }

    @SqlNullable
    @Description("extract part from url")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlExtractPath(@SqlType("varchar(x)") Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getPath());
    }

    @SqlNullable
    @Description("extract query from url")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlExtractQuery(@SqlType("varchar(x)") Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getQuery());
    }

    @SqlNullable
    @Description("extract fragment from url")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlExtractFragment(@SqlType("varchar(x)") Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getFragment());
    }

    @SqlNullable
    @Description("extract query parameter from url")
    @ScalarFunction
    @LiteralParameters({"x", "y"})
    @SqlType("varchar(x)")
    public static Slice urlExtractParameter(@SqlType("varchar(x)") Slice url, @SqlType("varchar(y)") Slice parameterName)
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
    @LiteralParameters({"x", "y"})
    @Constraint(variable = "y", expression = "min(2147483647, x * 12)")
    @SqlType("varchar(y)")
    public static Slice urlEncode(@SqlType("varchar(x)") Slice value)
    {
        Escaper escaper = UrlEscapers.urlFormParameterEscaper();
        return slice(escaper.escape(value.toStringUtf8()));
    }

    @Description("unescape a URL-encoded string")
    @ScalarFunction
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlDecode(@SqlType("varchar(x)") Slice value)
    {
        try {
            return slice(URLDecoder.decode(value.toStringUtf8(), UTF_8.name()));
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
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
