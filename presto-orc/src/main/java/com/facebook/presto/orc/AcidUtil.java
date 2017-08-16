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

import com.facebook.presto.orc.metadata.OrcType;

import java.util.ArrayList;
import java.util.List;

public final class AcidUtil
{
    private static final List<String> acidEventFieldNames = new ArrayList<String>();

    static {
        acidEventFieldNames.add("operation");
        acidEventFieldNames.add("originalTransaction");
        acidEventFieldNames.add("bucket");
        acidEventFieldNames.add("rowId");
        acidEventFieldNames.add("currentTransaction");
        acidEventFieldNames.add("row");
    }

    private AcidUtil()
    {
    }

    public static boolean isAcidSchema(OrcType type)
    {
        if (type.getOrcTypeKind().equals(OrcType.OrcTypeKind.STRUCT)) {
            List<String> rootFields = type.getFieldNames();
            if (acidEventFieldNames.equals(rootFields)) {
                return true;
            }
        }
        return false;
    }

    public static int getReaderFieldOffset(OrcType type)
    {
        return isAcidSchema(type) ? acidEventFieldNames.size() : 0;
    }
}
