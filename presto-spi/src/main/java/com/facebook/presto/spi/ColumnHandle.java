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
package com.facebook.presto.spi;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.spi.api.Experimental;

import java.util.List;

public interface ColumnHandle
{
    /**
     * Applies to columns of complex types: arrays, maps and structs. When a query
     * uses only some of the subfields, the engine provides the complete list of
     * required subfields and the connector is free to prune the rest.
     * <p>
     * Examples:
     *  - SELECT a[1], b['x'], x.y.z FROM t
     *  - SELECT a FROM t WHERE b['y'] > 10
     * <p>
     * Pruning must preserve the type of the values and support unmodified access.
     * <p>
     * - Pruning a struct means populating some of the members with null values.
     * - Pruning a map means dropping keys not listed in the required subfields.
     * - Pruning arrays means dropping values with indices larger than maximum
     * required index and filling in remaining non-required indices with nulls.
     */
    @Experimental
    default ColumnHandle withRequiredSubfields(List<Subfield> subfields)
    {
        return this;
    }
}
