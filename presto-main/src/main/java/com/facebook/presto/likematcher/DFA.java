package com.facebook.presto.likematcher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DFA
{
    private final State start;
    private final State failed;
    private List<State> states;
    private Map<Integer, List<Transition>> transitions;

    public DFA(State start, State failed, List<State> states, Map<Integer, List<Transition>> transitions)
    {
        this.start = Objects.requireNonNull(start, "start is null");
        this.failed = Objects.requireNonNull(failed, "failed is null");
        this.states = ImmutableList.copyOf(states);
        this.transitions = ImmutableMap.copyOf(transitions);
    }

    public State getStart()
    {
        return start;
    }

    public State getFailed()
    {
        return failed;
    }

    public List<State> getStates()
    {
        return states;
    }

    public Map<Integer, List<Transition>> getTransitions()
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
        private final State target;

        public Transition(int value, State target)
        {
            this.value = value;
            this.target = target;
        }

        public int getValue()
        {
            return value;
        }

        public State getTarget()
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
        private State start;
        private State failed;
        private final List<State> states = new ArrayList<>();
        private final Map<Integer, List<Transition>> transitions = new HashMap<>();

        public State addState(String label, boolean accept)
        {
            State state = new State(nextId++, label, accept);
            states.add(state);
            return state;
        }

        public State addStartState(String label, boolean accept)
        {
            if (start != null) {
                throw new IllegalStateException("Start state already set");
            }
            State state = addState(label, accept);
            start = state;
            return state;
        }

        public State addFailState()
        {
            if (failed != null) {
                throw new IllegalStateException("Fail state already set");
            }
            State state = addState("fail", false);
            failed = state;
            return state;
        }

        public void addTransition(State from, int value, State to)
        {
            transitions.computeIfAbsent(from.getId(), key -> new ArrayList<>())
                    .add(new Transition(value, to));
        }

        public DFA build()
        {
            return new DFA(start, failed, states, transitions);
        }
    }
}
