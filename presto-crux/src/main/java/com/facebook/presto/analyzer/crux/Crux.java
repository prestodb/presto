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
package com.facebook.presto.analyzer.crux;

import com.facebook.presto.analyzer.crux.tree.EmptyQuery;
import com.facebook.presto.analyzer.crux.tree.SemanticTree;

/**
 * TODO: A dummy implementation of Crux analyzer. This would be replaced with actual implementation.
 */
public class Crux
{
    private Crux()
    {
    }

    /**
     * // TODO: Returning an empty query for now, this should be replaced with actual crux call
     */
    public static SemanticTree sqlToTree(String query)
    {
        return new EmptyQuery(null);
    }
}
