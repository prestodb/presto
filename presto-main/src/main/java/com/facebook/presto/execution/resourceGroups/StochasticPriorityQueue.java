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
package com.facebook.presto.execution.resourceGroups;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

final class StochasticPriorityQueue<E>
        implements UpdateablePriorityQueue<E>
{
    private final Map<E, Node<E>> index = new HashMap<>();

    // This is a Fenwick tree, where each node has weight equal to the sum of its weight
    // and all its children's weights
    private Node<E> root;

    @Override
    public boolean addOrUpdate(E element, int priority)
    {
        checkArgument(priority > 0, "priority must be positive");
        if (root == null) {
            root = new Node<>(Optional.empty(), element);
            root.setTickets(priority);
            index.put(element, root);
            return true;
        }
        Node<E> node = index.get(element);
        if (node != null) {
            node.setTickets(priority);
            return false;
        }

        node = root.addNode(element, priority);
        index.put(element, node);
        return true;
    }

    @Override
    public boolean contains(E element)
    {
        return index.containsKey(element);
    }

    @Override
    public boolean remove(E element)
    {
        Node<E> node = index.remove(element);
        if (node == null) {
            return false;
        }
        if (node.isLeaf() && node.equals(root)) {
            root = null;
        }
        else if (node.isLeaf()) {
            node.remove();
        }
        else {
            // This is an intermediate node.  Instead of removing it directly, remove a leaf
            // and then replace the data in this node with the data in the leaf.  This way
            // we don't have to reorganize the tree structure.
            Node<E> leaf = root.findLeaf();
            leaf.remove();
            node.setTickets(leaf.getTickets());
            node.setValue(leaf.getValue());
            index.put(leaf.getValue(), node);
        }
        return true;
    }

    @Override
    public E poll()
    {
        if (root == null) {
            return null;
        }

        long winningTicket = ThreadLocalRandom.current().nextLong(root.getTotalTickets());
        Node<E> candidate = root;
        while (!candidate.isLeaf()) {
            long leftTickets = candidate.getLeft().map(Node::getTotalTickets).orElse(0L);

            if (winningTicket < leftTickets) {
                candidate = candidate.getLeft().get();
                continue;
            }
            else {
                winningTicket -= leftTickets;
            }

            if (winningTicket < candidate.getTickets()) {
                break;
            }
            else {
                winningTicket -= candidate.getTickets();
            }

            checkState(candidate.getRight().isPresent(), "Expected right node to contain the winner, but it does not exist");
            candidate = candidate.getRight().get();
        }
        checkState(winningTicket < candidate.getTickets(), "Inconsistent winner");

        E value = candidate.getValue();
        remove(value);
        return value;
    }

    @Override
    public E peek()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size()
    {
        return index.size();
    }

    @Override
    public boolean isEmpty()
    {
        return index.isEmpty();
    }

    private static final class Node<E>
    {
        private Optional<Node<E>> parent;
        private E value;
        private Optional<Node<E>> left = Optional.empty();
        private Optional<Node<E>> right = Optional.empty();
        private int tickets;
        private long totalTickets;
        private int descendants;

        private Node(Optional<Node<E>> parent, E value)
        {
            this.parent = parent;
            this.value = value;
        }

        public E getValue()
        {
            return value;
        }

        public void setValue(E value)
        {
            this.value = value;
        }

        public Optional<Node<E>> getLeft()
        {
            return left;
        }

        public Optional<Node<E>> getRight()
        {
            return right;
        }

        public long getTotalTickets()
        {
            return totalTickets;
        }

        public int getTickets()
        {
            return tickets;
        }

        public void setTickets(int tickets)
        {
            checkArgument(tickets > 0, "tickets must be positive");
            if (tickets == this.tickets) {
                return;
            }
            int ticketDelta = tickets - this.tickets;
            Node<E> node = this;
            // Update total tickets in this node and all ancestors
            while (node != null) {
                node.totalTickets += ticketDelta;
                node = node.parent.orElse(null);
            }
            this.tickets = tickets;
        }

        public boolean isLeaf()
        {
            return !left.isPresent() && !right.isPresent();
        }

        public Node<E> findLeaf()
        {
            int leftDecendants = left.map(node -> node.descendants).orElse(0);
            int rightDecendants = right.map(node -> node.descendants).orElse(0);

            if (leftDecendants == 0 && rightDecendants == 0) {
                return left.orElse(right.orElse(this));
            }
            if (leftDecendants > rightDecendants) {
                return left.get().findLeaf();
            }
            if (rightDecendants > leftDecendants) {
                return right.get().findLeaf();
            }
            // For ties just go left
            checkState(left.isPresent(), "Left child missing");
            return left.get().findLeaf();
        }

        public void remove()
        {
            checkState(parent.isPresent(), "Cannot remove root node");
            checkState(isLeaf(), "Can only remove leaf nodes");
            Node<E> parent = this.parent.get();
            if (parent.getRight().map(node -> node.equals(this)).orElse(false)) {
                parent.right = Optional.empty();
            }
            else {
                checkState(parent.getLeft().map(node -> node.equals(this)).orElse(false), "Inconsistent parent pointer");
                parent.left = Optional.empty();
            }
            while (parent != null) {
                parent.descendants--;
                parent.totalTickets -= tickets;
                parent = parent.parent.orElse(null);
            }
            this.parent = Optional.empty();
        }

        public Node<E> addNode(E value, int tickets)
        {
            // setTickets call in base case will update totalTickets
            descendants++;
            if (left.isPresent() && right.isPresent()) {
                // Keep the tree balanced when inserting
                if (left.get().descendants < right.get().descendants) {
                    return left.get().addNode(value, tickets);
                }
                else {
                    return right.get().addNode(value, tickets);
                }
            }

            Node<E> child = new Node<>(Optional.of(this), value);
            if (left.isPresent()) {
                right = Optional.of(child);
            }
            else {
                left = Optional.of(child);
            }
            child.setTickets(tickets);
            return child;
        }
    }
}
