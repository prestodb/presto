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
package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class PartitionItem
        extends Statement
{
    private final String key;
    private final String value;

    public PartitionItem(
            String key,
            String value)
    {
        checkNotNull(key, "key is null");
        checkNotNull(value, "value is null");

        this.key = key;
        this.value = value;
    }

    public String getKey()
    {
        return key;
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatement(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("key", key)
                .add("value", value)
                .toString();
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
        PartitionItem o = (PartitionItem) obj;
        return Objects.equal(key, o.key) &&
                Objects.equal(value, o.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(key, value);
    }
}
