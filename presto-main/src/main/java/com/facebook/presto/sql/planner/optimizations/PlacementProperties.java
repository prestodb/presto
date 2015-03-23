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
package com.facebook.presto.sql.planner.optimizations;

class PlacementProperties
{
    public enum Type
    {
        COORDINATOR_ONLY,
        SOURCE,
        ANY
    }

    private final Type type;

    public static PlacementProperties anywhere()
    {
        return new PlacementProperties(Type.ANY);
    }

    public static PlacementProperties source()
    {
        return new PlacementProperties(Type.SOURCE);
    }

    public static PlacementProperties coordinatorOnly()
    {
        return new PlacementProperties(Type.COORDINATOR_ONLY);
    }

    private PlacementProperties(Type type)
    {
        this.type = type;
    }

    public Type getType()
    {
        return type;
    }

    public String toString()
    {
        return type.toString();
    }
}
