package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;

import java.util.IdentityHashMap;

public class AnalysisContext
{
    private final SlotAllocator slotAllocator;
    private final AnalysisContext parent;

    private final IdentityHashMap<Subquery, AnalysisResult> inlineViews = new IdentityHashMap<>();
    private final IdentityHashMap<Relation, TupleDescriptor> tableDescriptors = new IdentityHashMap<>();

    public AnalysisContext()
    {
        this.parent = null;
        this.slotAllocator = new SlotAllocator();
    }

    public AnalysisContext(AnalysisContext parent)
    {
        this.parent = parent;
        this.slotAllocator = null;
    }

    public IdentityHashMap<Subquery, AnalysisResult> getInlineViews()
    {
        return inlineViews;
    }

    public void registerInlineView(Subquery node, AnalysisResult analysis)
    {
        inlineViews.put(node, analysis);
    }

    public void registerTable(Table table, TupleDescriptor descriptor)
    {
        tableDescriptors.put(table, descriptor);
    }

    public IdentityHashMap<Relation, TupleDescriptor> getTableDescriptors()
    {
        return tableDescriptors;
    }

    public SlotAllocator getSlotAllocator()
    {
        if (parent != null) {
            return parent.getSlotAllocator();
        }

        return slotAllocator;
    }
}
