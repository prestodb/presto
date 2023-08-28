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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkState;

public class NFA
{
    private final State start;
    private final State accept;
    private List<State> states;
    private Map<Integer, List<Transition>> transitions;

    public NFA(State start, State accept, List<State> states, Map<Integer, List<Transition>> transitions)
    {
        this.start = requireNonNull(start, "start is null");
        this.accept = requireNonNull(accept, "accept is null");
        this.states = ImmutableList.copyOf(states);
        this.transitions = ImmutableMap.copyOf(transitions);
    }

    public DFA toDfa()
    {
        Map<Set<NFA.State>, DFA.State> activeStates = new HashMap<>();

        DFA.Builder builder = new DFA.Builder();
        DFA.State failed = builder.addFailState();
        for (int i = 0; i < 256; i++) {
            builder.addTransition(failed, i, failed);
        }

        Set<NFA.State> initial = transitiveClosure(Collections.singleton((this.start)));
        Queue<Set<State>> queue = new ArrayDeque<>();
        queue.add(initial);

        DFA.State dfaStartState = builder.addStartState(makeLabel(initial), initial.contains(accept));
        activeStates.put(initial, dfaStartState);

        Set<Set<NFA.State>> visited = new HashSet<>();
        while (!queue.isEmpty()) {
            Set<NFA.State> current = queue.poll();

            if (!visited.add(current)) {
                continue;
            }

            // For each possible byte value...
            for (int byteValue = 0; byteValue < 256; byteValue++) {
                Set<NFA.State> next = new HashSet<>();
                for (NFA.State nfaState : current) {
                    for (Transition transition : transitions(nfaState)) {
                        Condition condition = transition.getCondition();
                        State target = states.get(transition.getTarget());

                        if (condition instanceof Value) {
                            Value valueTransition = (Value) condition;
                            if (valueTransition.getValue() == (byte) byteValue) {
                                next.add(target);
                            }
                        }
                        else if (condition instanceof Prefix) {
                            Prefix prefixTransition = (Prefix) condition;
                            if (byteValue >>> (8 - prefixTransition.getBits()) == prefixTransition.getPrefix()) {
                                next.add(target);
                            }
                        }
                    }
                }

                DFA.State from = activeStates.get(current);
                DFA.State to = failed;
                if (!next.isEmpty()) {
                    Set<NFA.State> closure = transitiveClosure(next);
                    to = activeStates.computeIfAbsent(closure, nfaStates -> builder.addState(makeLabel(nfaStates), nfaStates.contains(accept)));
                    queue.add(closure);
                }
                builder.addTransition(from, byteValue, to);
            }
        }

        return builder.build();
    }

    private List<Transition> transitions(State state)
    {
        return transitions.getOrDefault(state.id, ImmutableList.of());
    }

    /**
     * Traverse epsilon transitions to compute the reachable set of states
     */
    private Set<State> transitiveClosure(Set<State> states)
    {
        Set<State> result = new HashSet<>();

        Queue<State> queue = new ArrayDeque<>(states);
        while (!queue.isEmpty()) {
            State state = queue.poll();

            if (result.contains(state)) {
                continue;
            }

            transitions(state).stream()
                    .filter(transition -> transition.getCondition() instanceof Epsilon)
                    .forEach(transition -> {
                        State target = this.states.get(transition.getTarget());
                        result.add(target);
                        queue.add(target);
                    });
        }

        result.addAll(states);

        return result;
    }

    private String makeLabel(Set<State> states)
    {
        return "{" + states.stream()
                .map(NFA.State::getId)
                .map(Object::toString)
                .sorted()
                .collect(Collectors.joining(",")) + "}";
    }

    public static class Builder
    {
        private int nextId;
        private State start;
        private State accept;
        private final List<State> states = new ArrayList<>();
        private final Map<Integer, List<Transition>> transitions = new HashMap<>();

        public State addState()
        {
            State state = new State(nextId++);
            states.add(state);
            return state;
        }

        public State addStartState()
        {
            checkState(start == null, "Start state is already set");
            start = addState();
            return start;
        }

        public void setAccept(State state)
        {
            checkState(accept == null, "Accept state is already set");
            accept = state;
        }

        public void addTransition(State from, Condition condition, State to)
        {
            transitions.computeIfAbsent(from.getId(), key -> new ArrayList<>())
                    .add(new Transition(to.getId(), condition));
        }

        public NFA build()
        {
            return new NFA(start, accept, states, transitions);
        }
    }

    public static class State
    {
        private final int id;

        public State(int id)
        {
            this.id = id;
        }

        public int getId()
        {
            return id;
        }

        @Override
        public String toString()
        {
            return "(" + id + ")";
        }
    }

    public static class Transition
    {
        private final int target;
        private final Condition condition;

        public Transition(int target, Condition condition)
        {
            this.target = target;
            this.condition = condition;
        }

        public int getTarget()
        {
            return target;
        }

        public Condition getCondition()
        {
            return condition;
        }
    }

    public interface Condition
    {
    }

    public static class Epsilon
            implements Condition
    {
    }

    public static class Value
            implements Condition
    {
        private final byte value;

        public Value(byte value)
        {
            this.value = value;
        }

        public byte getValue()
        {
            return value;
        }
    }

    public static class Prefix
            implements Condition
    {
        private final int prefix;
        private final int bits;

        public Prefix(int prefix, int bits)
        {
            this.prefix = prefix;
            this.bits = bits;
        }

        public int getPrefix()
        {
            return prefix;
        }

        public int getBits()
        {
            return bits;
        }
    }
}
