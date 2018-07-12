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
package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class FunctionHandle
{
    private final Signature signature;
    private final String catalog;
    private final String schema;

    @JsonCreator
    public FunctionHandle(
            @JsonProperty("signature") Signature signature,
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema)
    {
        this.signature = requireNonNull(signature, "signature is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    @JsonProperty
    public Signature getSignature()
    {
        return signature;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signature, catalog, schema);
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
        FunctionHandle other = (FunctionHandle) obj;
        return Objects.equals(this.signature, other.signature) &&
                Objects.equals(this.catalog, other.catalog) &&
                Objects.equals(this.schema, other.schema);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("signature", signature)
                .add("catalog", catalog)
                .add("schema", schema)
                .toString();
    }
}
