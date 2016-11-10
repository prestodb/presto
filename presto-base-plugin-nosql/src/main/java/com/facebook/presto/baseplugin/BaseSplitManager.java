package com.facebook.presto.baseplugin;

import com.facebook.presto.baseplugin.predicate.BaseComparisonOperator;
import com.facebook.presto.baseplugin.predicate.BasePredicate;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseSplitManager implements ConnectorSplitManager {
    private final NodeManager nodeManager;
    private final BaseConnectorInfo baseConnectorInfo;
    private final BaseProvider baseProvider;

    private final Logger logger = Logger.get(BaseSplitManager.class);

    @Inject
    public BaseSplitManager(NodeManager nodeManager, BaseConnectorInfo baseConnectorInfo, BaseProvider baseProvider)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.baseConnectorInfo = requireNonNull(baseConnectorInfo, "baseConnectorInfo is null");
        this.baseProvider = requireNonNull(baseProvider, "baseProvider is null");
    }

    /**
     * breaks up the predicate across the splits depending on the predicate characteristics
     * @param predicate the predicate to break up
     * @param splits the fixed list of splits to break up the predicate across
     */
    public void allocatePredicateToSplits(BasePredicate predicate, List<? extends BaseSplit> splits)
    {
        if(!splits.isEmpty()) {
            switch (predicate.getBaseComparisonOperator()) {
                case IN:
                    int partitionSize = (int) Math.ceil(((double) predicate.getValues().size()) / splits.size());
                    logger.info("Partition size of %s using %s number of splits for predicate with %s number of values", partitionSize, splits.size(), predicate.getValues().size());
                    List<BasePredicate> predicateList = Lists.partition(
                            predicate.getValues(),
                            partitionSize
                    ).stream().map(x -> new BasePredicate(predicate.getBaseColumnHandle(), BaseComparisonOperator.IN, x)).collect(Collectors.toList());
                    logger.info("Broke original predicate into %s number of predicates", predicateList.size());
                    //splits.forEach(x -> x.getPredicates().add(predicateIterator.next()));
                    int splitIndex = 0;
                    Iterator<BasePredicate> predicateIterator = predicateList.iterator();
                    while (predicateIterator.hasNext()) {
                        splits.get(splitIndex).getPredicates().add(predicateIterator.next());
                        if(splits.size() - 1 > splitIndex) {
                            splitIndex++;
                        }
                    }
                    break;
                case BETWEEN:
                    Double start = ((Number) predicate.getValues().get(0)).doubleValue();
                    Double end = ((Number) predicate.getValues().get(1)).doubleValue();
                    Double incrementSize = (end - start) / splits.size();
                    for (BaseSplit split : splits) {
                        split.getPredicates().add(new BasePredicate(predicate.getBaseColumnHandle(), BaseComparisonOperator.BETWEEN, ImmutableList.of(start, start + incrementSize)));
                        start += incrementSize;
                    }
                    break;
                default:
                    if (!splits.isEmpty()) {
                        splits.forEach(s -> s.getPredicates().add(predicate));
                    }
            }
        }
    }


    @Override
    /**
     * returns a list of splits for the coordinator to handle
     * @return a representation of a source and the splits associated with it
     * @param transactionHandle contains information about the SQL transaction
     * @param session contains query specific meta-information
     * @param layout contains information about the table, predicates, and additional constraint
     */
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        BaseTableLayoutHandle baseTableLayout = (BaseTableLayoutHandle) layout;

        //creates as many splits as there are active nodes in the cluster. EX: 3 nodes -> 3 splits
        final List<BaseSplit> baseSplits = nodeManager.getWorkerNodes().stream()
                .map(x -> new BaseSplit(ImmutableList.of(x.getHostAndPort()), baseTableLayout.getTable().getSchemaTableName(), new ArrayList<>()))
                .collect(Collectors.toList());

        //turns the columnDomains from the baseTableLayout into basePredicates
        List<BasePredicate> basePredicates = baseTableLayout.getSummary().getColumnDomains().get().stream()
                .map(BasePredicate::fromColumnDomain)
                .collect(Collectors.toList());

        //modify list of predicates going into be split
        basePredicates = updateBasePredicates(basePredicates, baseTableLayout.getTable(), baseProvider, session);

        //allocates predicates to splits based on whether predicates have finite bounds or not
        basePredicates.forEach(p -> allocatePredicateToSplits(p, baseSplits));

        //allows updates to predicate-filled splits before they're sent back to coordinator
        return new FixedSplitSource(updateBaseSplits(baseSplits, baseProvider, session));
    }

    /**
     * Should override this method
     * @param basePredicates
     * @param tableHandle
     * @param baseProvider
     * @param session
     * @return
     */
    public List<BasePredicate> updateBasePredicates(List<BasePredicate> basePredicates, BaseTableHandle tableHandle, BaseProvider baseProvider, ConnectorSession session){
        return basePredicates;
    }

    /**
     * Should override this method
     * @param baseSplits
     * @param baseProvider
     * @return
     */
    public List<BaseSplit> updateBaseSplits(List<BaseSplit> baseSplits, BaseProvider baseProvider, ConnectorSession session){
        return baseSplits;
    }
}
