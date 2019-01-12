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
package io.prestosql.orc;

import java.io.IOException;

import static java.lang.String.format;

public class OrcCorruptionException
        extends IOException
{
    public OrcCorruptionException(OrcDataSourceId orcDataSourceId, String message)
    {
        this(orcDataSourceId, "%s", message);
    }

    public OrcCorruptionException(OrcDataSourceId orcDataSourceId, String messageFormat, Object... args)
    {
        super(formatMessage(orcDataSourceId, messageFormat, args));
    }

    public OrcCorruptionException(Throwable cause, OrcDataSourceId orcDataSourceId, String messageFormat, Object... args)
    {
        super(formatMessage(orcDataSourceId, messageFormat, args), cause);
    }

    private static String formatMessage(OrcDataSourceId orcDataSourceId, String messageFormat, Object[] args)
    {
        return "Malformed ORC file. " + format(messageFormat, args) + " [" + orcDataSourceId + "]";
    }
}
