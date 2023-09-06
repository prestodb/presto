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
package com.facebook.presto.likematcher;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.List;

public class DFA
{
    private final int start;
    private final IntArrayList acceptStates;
    private final List<List<Transition>> transitions;

    // Constructor
    public DFA(int start, IntArrayList acceptStates, List<List<Transition>> transitions)
    {
        this.start = start;
        this.acceptStates = acceptStates;
        this.transitions = transitions;
    }

    // Getters
    public int getStart()
    {
        return start;
    }

    public IntArrayList getAcceptStates()
    {
        return acceptStates;
    }

    public List<List<Transition>> getTransitions()
    {
        return transitions;
    }

    public List<Transition> transitions(State state)
    {
        return transitions.get(state.getId());
    }

    public static class State
    {
        private final int id;
        private final String label;
        private final boolean accept;

        public State(int id, String label, boolean accept)
        {
            this.id = id;
            this.label = label;
            this.accept = accept;
        }

        public int getId()
        {
            return id;
        }

        public String getLabel()
        {
            return label;
        }

        public boolean isAccept()
        {
            return accept;
        }

        @Override
        public String toString()
        {
            return String.format("%d:%s%s", id, accept ? "*" : "", label);
        }
    }

    public static class Transition
    {
        private final int value;
        private final int target;

        public Transition(int value, int target)
        {
            this.value = value;
            this.target = target;
        }

        public int getValue()
        {
            return value;
        }

        public int getTarget()
        {
            return target;
        }

        @Override
        public String toString()
        {
            return String.format("-[%s]-> %s", value, target);
        }
    }

    public static class Builder
    {
        private int nextId;
        private int start;
        private final IntArrayList acceptStates = new IntArrayList();
        private final List<List<Transition>> transitions = new ArrayList<>();

        public int addState(boolean accept)
        {
            int state = nextId++;
            transitions.add(new ArrayList<>());
            if (accept) {
                acceptStates.add(state);
            }
            return state;
        }

        public int addStartState(boolean accept)
        {
            start = addState(accept);
            return start;
        }

        public void addTransition(int from, int value, int to)
        {
            transitions.get(from).add(new Transition(value, to));
        }

        public DFA build()
        {
            return new DFA(start, acceptStates, transitions);
        }
    }
}
