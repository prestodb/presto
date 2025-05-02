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
package com.facebook.presto.verifier.checksum;

import jakarta.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class SimpleColumnChecksum
        extends ColumnChecksum
{
    private final Object checksum;
    private final Optional<FloatingPointColumnChecksum> asDoubleChecksum;

    public SimpleColumnChecksum(@Nullable Object checksum, Optional<FloatingPointColumnChecksum> asDoubleChecksum)
    {
        this.checksum = checksum;
        this.asDoubleChecksum = asDoubleChecksum;
    }

    @Nullable
    public Object getChecksum()
    {
        return checksum;
    }

    public FloatingPointColumnChecksum getAsDoubleChecksum()
    {
        checkArgument(asDoubleChecksum.isPresent(), "Expect as-double checksum to be present, but it is not");

        return asDoubleChecksum.get();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SimpleColumnChecksum o = (SimpleColumnChecksum) obj;
        return Objects.equals(checksum, o.checksum) &&
                Objects.equals(asDoubleChecksum, o.asDoubleChecksum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(checksum, asDoubleChecksum);
    }

    @Override
    public String toString()
    {
        if (asDoubleChecksum.isPresent()) {
            return asDoubleChecksum.get().toString();
        }
        else {
            return format("checksum: %s", checksum);
        }
    }
}
