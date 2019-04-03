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

import java.util.List;

public interface ColumnHandle
{
    default boolean supportsSubfieldTupleDomain()
    {
        return false;
    }

    /* Returns a ColumnHandle that refers to the subfield at
     * 'path'. Such a ColumnHandle may occur as a key in a TupleDomain
     * for filtering non-top level columns */
    default ColumnHandle createSubfieldColumnHandle(SubfieldPath path)
    {
        throw new UnsupportedOperationException();
    }

    default SubfieldPath getSubfieldPath()
    {
        return null;
    }

    default List<SubfieldPath> getReferencedSubfields()
    {
        return null;
    }
}
