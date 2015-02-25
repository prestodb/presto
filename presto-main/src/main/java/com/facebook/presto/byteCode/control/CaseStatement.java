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
package com.facebook.presto.byteCode.control;

import com.facebook.presto.byteCode.instruction.LabelNode;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class CaseStatement
        implements Comparable<CaseStatement>
{
    public static CaseStatement caseStatement(int key, LabelNode label)
    {
        return new CaseStatement(label, key);
    }

    private final int key;
    private final LabelNode label;

    CaseStatement(LabelNode label, int key)
    {
        this.label = label;
        this.key = key;
    }

    public int getKey()
    {
        return key;
    }

    public LabelNode getLabel()
    {
        return label;
    }

    @Override
    public int compareTo(CaseStatement o)
    {
        return Integer.compare(key, o.key);
    }

    @Override
    public int hashCode()
    {
        return key;
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
        CaseStatement other = (CaseStatement) obj;
        return Objects.equals(this.key, other.key);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("key", key)
                .add("label", label)
                .toString();
    }
}
