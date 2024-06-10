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
package com.facebook.presto.common;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.util.ConfigUtil;

import java.util.Locale;
import java.util.Objects;

import static com.facebook.presto.common.constant.ConfigConstants.ENABLE_MIXED_CASE_SUPPORT;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class CatalogSchemaName
{
    // TODO: Move out this class. Ideally this class should not be in presto-common module.

    private final String catalogName;
    private final String schemaName;

    @ThriftConstructor
    public CatalogSchemaName(String catalogName, String schemaName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null").toLowerCase(Locale.ENGLISH);
        boolean enableMixedCaseSupport = ConfigUtil.getConfig(ENABLE_MIXED_CASE_SUPPORT);
        this.schemaName = enableMixedCaseSupport ? requireNonNull(schemaName, "schemaName is null") :
                requireNonNull(schemaName.toLowerCase(Locale.ENGLISH), "schemaName is null");
    }

    @ThriftField(1)
    public String getCatalogName()
    {
        return catalogName;
    }

    @ThriftField(2)
    public String getSchemaName()
    {
        return schemaName;
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
        CatalogSchemaName that = (CatalogSchemaName) obj;
        return Objects.equals(catalogName, that.catalogName) &&
                Objects.equals(schemaName, that.schemaName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName);
    }

    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName;
    }
}
