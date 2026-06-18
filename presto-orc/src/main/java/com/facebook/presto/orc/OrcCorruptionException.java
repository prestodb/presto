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
package com.facebook.presto.orc;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.lang.String.format;

public class OrcCorruptionException
        extends UncheckedIOException
{
    public OrcCorruptionException(OrcDataSourceId orcDataSourceId, String message)
    {
        this(orcDataSourceId, "%s", message);
    }

    public OrcCorruptionException(OrcDataSourceId orcDataSourceId, String messageFormat, Object... args)
    {
        super(new IOException(formatMessage(orcDataSourceId, messageFormat, args)));
    }

    public OrcCorruptionException(Throwable cause, OrcDataSourceId orcDataSourceId, String messageFormat, Object... args)
    {
        super(formatMessage(orcDataSourceId, messageFormat, args), new IOException(cause));
    }

    private static String formatMessage(OrcDataSourceId orcDataSourceId, String messageFormat, Object[] args)
    {
        return "Malformed ORC file. " + format(messageFormat, args) + " [" + orcDataSourceId + "]";
    }
}
