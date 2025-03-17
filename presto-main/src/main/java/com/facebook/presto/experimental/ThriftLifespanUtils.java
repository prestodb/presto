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
package com.facebook.presto.experimental;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.experimental.auto_gen.ThriftLifespan;

public class ThriftLifespanUtils
{
    private ThriftLifespanUtils() {}

    public static Lifespan toLifespan(ThriftLifespan thriftLifespan)
    {
        if (thriftLifespan == null) {
            return null;
        }
        return new Lifespan(thriftLifespan.isGrouped(), thriftLifespan.getGroupId());
    }

    public static ThriftLifespan fromLifespan(Lifespan lifespan)
    {
        if (lifespan == null) {
            return null;
        }
        return new ThriftLifespan(lifespan.isGrouped(), lifespan.getId());
    }
}
