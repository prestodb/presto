package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableMap;

class AnalysisContext
{
    private final Session session;
    private final SymbolAllocator symbolAllocator;

    private final IdentityHashMap<Subquery, AnalysisResult> inlineViews = new IdentityHashMap<>();
    private final IdentityHashMap<Relation, TupleDescriptor> tableDescriptors = new IdentityHashMap<>();
    private final IdentityHashMap<Relation, TableMetadata> tableMetadata = new IdentityHashMap<>();
    private final IdentityHashMap<Join, List<AnalyzedJoinClause>> joinCriteria = new IdentityHashMap<>();
    private final Map<String, Subquery> withQueries = new HashMap<>();
    private final IdentityHashMap<Table, Subquery> withQueryReferences = new IdentityHashMap<>();

    public AnalysisContext(Session session)
    {
        this(session, new SymbolAllocator());
    }

    public AnalysisContext(AnalysisContext context)
    {
        this(context.getSession(), context.getSymbolAllocator());
        inlineViews.putAll(context.getInlineViews());
        withQueries.putAll(context.getWithQueries());
    }

    private AnalysisContext(Session session, SymbolAllocator symbolAllocator)
    {
        this.session = checkNotNull(session, "session is null");
        this.symbolAllocator = checkNotNull(symbolAllocator, "symbolAllocator is null");
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

    public Map<String, Subquery> getWithQueries()
    {
        return unmodifiableMap(withQueries);
    }

    public void registerWithQuery(String name, Subquery subquery)
    {
        withQueries.put(name, subquery);
    }

    /**
     * We really want to expose an unmodifiable identity map here. Unfortunately there's no such a thing, so we expose the raw reference.
     * Callers should *not* modify its contents.
     */
    public IdentityHashMap<Table, Subquery> getWithQueryReferences()
    {
        return withQueryReferences;
    }

    public void registerWithQueryReference(Table table, Subquery subquery)
    {
        withQueryReferences.put(table, subquery);
    }

    public Session getSession()
    {
        return session;
    }
}
