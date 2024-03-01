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
package com.facebook.presto.thrift.api.connector;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnMetadata;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.thrift.api.connector.NameValidationUtils.checkValidName;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftColumnMetadata
{
    private final String name;
    private final String type;
    private final String comment;
    private final boolean hidden;

    @ThriftConstructor
    public PrestoThriftColumnMetadata(String name, String type, @Nullable String comment, boolean hidden)
    {
        this.name = checkValidName(name);
        this.type = requireNonNull(type, "type is null");
        this.comment = comment;
        this.hidden = hidden;
    }

    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @ThriftField(2)
    public String getType()
    {
        return type;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public String getComment()
    {
        return comment;
    }

    @ThriftField(4)
    public boolean isHidden()
    {
        return hidden;
    }

    public ColumnMetadata toColumnMetadata(TypeManager typeManager)
    {
        return new ColumnMetadata(
                name,
                typeManager.getType(parseTypeSignature(type)),
                comment,
                hidden);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftColumnMetadata other = (PrestoThriftColumnMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.comment, other.comment) &&
                this.hidden == other.hidden;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment, hidden);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("comment", comment)
                .add("hidden", hidden)
                .toString();
    }
}
