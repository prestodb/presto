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
package com.facebook.presto.matching.example.rel;

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.Property;

import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.matching.Property.property;

public class Patterns
{
    private Patterns() {}

    public static Pattern<JoinNode> join()
    {
        return typeOf(JoinNode.class);
    }

    public static Property<JoinNode, RelNode> build()
    {
        return property("build", JoinNode::getBuild);
    }

    public static Property<JoinNode, RelNode> probe()
    {
        return property("probe", JoinNode::getProbe);
    }

    public static Pattern<ScanNode> scan()
    {
        return typeOf(ScanNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<RelNode> plan()
    {
        return typeOf(RelNode.class);
    }

    public static Pattern<ProjectNode> project()
    {
        return typeOf(ProjectNode.class);
    }

    public static Property<ScanNode, String> tableName()
    {
        return property("tableName", ScanNode::getTableName);
    }

    public static Property<SingleSourceRelNode, RelNode> source()
    {
        return property("source", SingleSourceRelNode::getSource);
    }
}
