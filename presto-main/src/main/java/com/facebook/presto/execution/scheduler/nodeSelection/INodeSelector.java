package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.presto.metadata.InternalNode;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.OptionalLong;

/**
 * Represents algorithm to select nodes from a set of candidates
 * based on certain user provided hint.
 */
public interface INodeSelector
{
    /**
     * Selects the elements from candidate nodes and returns based on
     * hint passed.
     *
     * @param candidates Nodes to select from
     * @param hint Hint to the selection algorithm
     * @return List of selected nodes.
     */
    List<InternalNode> select(List<InternalNode> candidates, NodeSelectionHint hint);

    /**
     * Class helps to provide suitable hint to the selection algorithm
     * in the NodeSelector.
     */
    class NodeSelectionHint
    {
        private final OptionalLong limit;
        private final boolean includeCoordinator;

        public NodeSelectionHint(OptionalLong limit, boolean includeCoordinator) {this.limit = limit;
            this.includeCoordinator = includeCoordinator;
        }

        public OptionalLong getLimit()
        {
            return limit;
        }

        public boolean canIncludeCoordinator() {
            return includeCoordinator;
        }

        public static Builder newBuilder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private OptionalLong limit = OptionalLong.empty();
            private boolean includeCoordinator = true;

            public Builder limit(long limit)
            {
                Preconditions.checkArgument(limit > 0, "Limit must be positive");
                this.limit = OptionalLong.of(limit);
                return this;
            }

            public Builder includeCoordinator(boolean flag)
            {
                this.includeCoordinator = flag;
                return this;
            }

            public NodeSelectionHint build()
            {
                return new NodeSelectionHint(limit, includeCoordinator);
            }
        }
    }
}
