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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;

import static java.lang.String.format;

public class OrcCorruptionException
        extends PrestoException
{
    public OrcCorruptionException(String message)
    {
        super(StandardErrorCode.CORRUPT_DATA, message);
    }

    public OrcCorruptionException(String messageFormat, Object... args)
    {
        super(StandardErrorCode.CORRUPT_DATA, format(messageFormat, args));
    }

    public OrcCorruptionException(Throwable cause, String messageFormat, Object... args)
    {
        super(StandardErrorCode.CORRUPT_DATA, format(messageFormat, args), cause);
    }
}
