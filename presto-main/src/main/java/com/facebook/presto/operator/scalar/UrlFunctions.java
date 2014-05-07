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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import static com.google.common.base.Strings.nullToEmpty;

public final class UrlFunctions
{
    private static final Splitter QUERY_SPLITTER = Splitter.on('&');
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);

    private UrlFunctions() {}

    @Nullable
    @Description("extract protocol from url")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice urlExtractProtocol(@SqlType(VarcharType.class) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getScheme());
    }

    @Nullable
    @Description("extract host from url")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice urlExtractHost(@SqlType(VarcharType.class) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getHost());
    }

    @Nullable
    @Description("extract port from url")
    @ScalarFunction
    @SqlType(BigintType.class)
    public static Long urlExtractPort(@SqlType(VarcharType.class) Slice url)
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
    @SqlType(VarcharType.class)
    public static Slice urlExtractPath(@SqlType(VarcharType.class) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getPath());
    }

    @Nullable
    @Description("extract query from url")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice urlExtractQuery(@SqlType(VarcharType.class) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getQuery());
    }

    @Nullable
    @Description("extract fragment from url")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice urlExtractFragment(@SqlType(VarcharType.class) Slice url)
    {
        URI uri = parseUrl(url);
        return (uri == null) ? null : slice(uri.getFragment());
    }

    @Nullable
    @Description("extract query parameter from url")
    @ScalarFunction
    @SqlType(VarcharType.class)
    public static Slice urlExtractParameter(@SqlType(VarcharType.class) Slice url, @SqlType(VarcharType.class) Slice parameterName)
    {
        URI uri = parseUrl(url);
        if ((uri == null) || (uri.getQuery() == null)) {
            return null;
        }

        Slice query = slice(uri.getQuery());
        String parameter = parameterName.toString(Charsets.UTF_8);
        Iterable<String> queryArgs = QUERY_SPLITTER.split(query.toString(Charsets.UTF_8));

        for (String queryArg : queryArgs) {
            Iterator<String> arg = ARG_SPLITTER.split(queryArg).iterator();
            if (arg.next().equals(parameter)) {
                if (arg.hasNext()) {
                    return Slices.copiedBuffer(arg.next(), Charsets.UTF_8);
                }
                // first matched key is empty
                return Slices.EMPTY_SLICE;
            }
        }

        // no key matched
        return null;
    }

    private static Slice slice(@Nullable String s)
    {
        return Slices.copiedBuffer(nullToEmpty(s), Charsets.UTF_8);
    }

    @Nullable
    private static URI parseUrl(Slice url)
    {
        try {
            return new URI(url.toString(Charsets.UTF_8));
        }
        catch (URISyntaxException e) {
            return null;
        }
    }
}
