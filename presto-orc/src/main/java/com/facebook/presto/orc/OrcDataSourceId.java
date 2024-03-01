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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class OrcDataSourceId
{
    private final String id;

    public OrcDataSourceId(String id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrcDataSourceId that = (OrcDataSourceId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        return id;
    }

    /**
     * Add the ORC file path to an exception to provide mode debug context.
     */
    public void attachToException(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        throwable.addSuppressed(new OrcDataSourceIdStackInfoException(this));
    }

    /**
     * This exception is supposed to provide extra context by being used as a
     * suppressed exception attached to a main exception.
     * <p>
     * Attaching as a suppressed exception allows to avoid modification
     * of top level exception just for the sake of providing more debug info.
     */
    private static class OrcDataSourceIdStackInfoException
            extends RuntimeException
    {
        public OrcDataSourceIdStackInfoException(OrcDataSourceId orcDataSourceId)
        {
            super("Failed to read ORC file: " + orcDataSourceId);
            this.setStackTrace(new StackTraceElement[0]); // we are not interested in the stack
        }
    }
}
