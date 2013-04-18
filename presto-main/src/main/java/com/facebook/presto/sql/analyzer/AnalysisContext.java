package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

class AnalysisContext
{
    private final Session session;
    private final SymbolAllocator symbolAllocator;

    private final IdentityHashMap<Subquery, AnalysisResult> inlineViews = new IdentityHashMap<>();
    private final IdentityHashMap<Relation, TupleDescriptor> tableDescriptors = new IdentityHashMap<>();
    private final IdentityHashMap<Relation, TableMetadata> tableMetadata = new IdentityHashMap<>();
    private final IdentityHashMap<Join, List<AnalyzedJoinClause>> joinCriteria = new IdentityHashMap<>();
    private final IdentityHashMap<QualifiedTableName, AnalysisResult> destinations = new IdentityHashMap<>();

    public AnalysisContext(Session session)
    {
        this(session, new SymbolAllocator());
    }

    public AnalysisContext(Session session, SymbolAllocator symbolAllocator)
    {
        this.session = session;
        this.symbolAllocator = symbolAllocator;
    }

    /**
     * We really want to expose an unmodifiable identity map here. Unfortunately there's no such a thing, so we expose the raw reference.
     * Callers should *not* modify its contents.
     */
    IdentityHashMap<Subquery, AnalysisResult> getInlineViews()
    {
        return inlineViews;
    }

    public void registerInlineView(Subquery node, AnalysisResult analysis)
    {
        inlineViews.put(node, analysis);
    }

    public void registerTable(Table table, TupleDescriptor descriptor, TableMetadata metadata)
    {
        tableDescriptors.put(table, descriptor);
        tableMetadata.put(table, metadata);
    }

    /**
     * We really want to expose an unmodifiable identity map here. Unfortunately there's no such a thing, so we expose the raw reference.
     * Callers should *not* modify its contents.
     */
    IdentityHashMap<Relation, TupleDescriptor> getTableDescriptors()
    {
        return tableDescriptors;
    }

    /**
     * We really want to expose an unmodifiable identity map here. Unfortunately there's no such a thing, so we expose the raw reference.
     * Callers should *not* modify its contents.
     */
    IdentityHashMap<Relation, TableMetadata> getTableMetadata()
    {
        return tableMetadata;
    }

    /**
     * We really want to expose an unmodifiable identity map here. Unfortunately there's no such a thing, so we expose the raw reference.
     * Callers should *not* modify its contents.
     */
    IdentityHashMap<Join, List<AnalyzedJoinClause>> getJoinCriteria()
    {
        return joinCriteria;
    }

    public SymbolAllocator getSymbolAllocator()
    {
        return symbolAllocator;
    }

    public Map<Symbol, Type> getSymbols()
    {
        return symbolAllocator.getTypes();
    }

    public void registerJoin(Join node, List<AnalyzedJoinClause> criteria)
    {
        joinCriteria.put(node, criteria);
    }

    public void addDestination(QualifiedTableName destination, AnalysisResult result)
    {
        checkNotNull(destination, "destination is null");
        checkNotNull(result, "result is null");
        this.destinations.put(destination, result);
    }

    IdentityHashMap<QualifiedTableName, AnalysisResult> getDestinations()
    {
        return destinations;
    }

    public Session getSession()
    {
        return session;
    }
}
