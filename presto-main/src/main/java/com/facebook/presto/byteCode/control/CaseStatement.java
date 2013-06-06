/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.byteCode.control;

import com.google.common.base.Objects;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.primitives.Ints;

import javax.annotation.concurrent.Immutable;

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
        return Ints.compare(key, o.key);
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
        final CaseStatement other = (CaseStatement) obj;
        return Objects.equal(this.key, other.key);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("key", key)
                .add("label", label)
                .toString();
    }
}
