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
package com.facebook.presto.sql.analyzer.crux;

import java.util.Collections;
import java.util.List;

/**
 * TODO: A dummy implementation of Crux analyzer. This would be replaced with actual implementation.
 */
public class Crux
{
    private Crux()
    {
    }

    public static SemanticTree sqlToTree(String query)
    {
        if (query.equalsIgnoreCase("SELECT 10")) {
            LiteralExpression literalExpression = new LiteralExpression(null, Type.INT8, 10);
            List<String> names = Collections.singletonList("");
            SelectItem selectItem = new SelectItem(null, names, literalExpression, null);
            List<SelectItem> selectItems = Collections.singletonList(selectItem);

            EmptyQuery emptyQuery = new EmptyQuery(null);

            return new SelectQuery(null, emptyQuery, selectItems);
        }

        return new EmptyQuery(null);
    }
}
