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

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

class DenseDfaMatcher
        implements Matcher
{
    public static final int FAIL_STATE = -1;

    private final List<Pattern> pattern;
    private final int start;
    private final int end;
    private final boolean exact;

    private volatile DenseDfa matcher;

    public DenseDfaMatcher(List<Pattern> pattern, int start, int end, boolean exact)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.start = start;
        this.end = end;
        this.exact = exact;
    }

    @Override
    public boolean match(byte[] input, int offset, int length)
    {
        DenseDfa matcher = this.matcher;
        if (matcher == null) {
            matcher = DenseDfa.newInstance(pattern, start, end);
            this.matcher = matcher;
        }

        if (exact) {
            return matcher.exactMatch(input, offset, length);
        }

        return matcher.prefixMatch(input, offset, length);
    }

    private static class DenseDfa
    {
        // The DFA is encoded as a sequence of transitions for each possible byte value for each state.
        // I.e., 256 transitions per state.
        // The content of the transitions array is the base offset into
        // the next state to follow. I.e., the desired state * 256
        private final int[] transitions;

        // The starting state
        private final int start;

        // For each state, whether it's an accepting state
        private final boolean[] accept;

        public static DenseDfa newInstance(List<Pattern> pattern, int start, int end)
        {
            DFA dfa = makeNfa(pattern, start, end).toDfa();

            int[] transitions = new int[dfa.getTransitions().size() * 256];
            Arrays.fill(transitions, FAIL_STATE);

            for (int state = 0; state < dfa.getTransitions().size(); state++) {
                for (DFA.Transition transition : dfa.getTransitions().get(state)) {
                    transitions[state * 256 + transition.getValue()] = transition.getTarget() * 256;
                }
            }
            boolean[] accept = new boolean[dfa.getTransitions().size()];
            for (int state : dfa.getAcceptStates()) {
                accept[state] = true;
            }

            return new DenseDfa(transitions, dfa.getStart(), accept);
        }

        private DenseDfa(int[] transitions, int start, boolean[] accept)
        {
            this.transitions = transitions;
            this.start = start;
            this.accept = accept;
        }

        /**
         * Returns a positive match when the final state after all input has been consumed is an accepting state
         */
        public boolean exactMatch(byte[] input, int offset, int length)
        {
            int state = start << 8;
            for (int i = offset; i < offset + length; i++) {
                byte inputByte = input[i];
                state = transitions[state | (inputByte & 0xFF)];

                if (state == FAIL_STATE) {
                    return false;
                }
            }

            return accept[state >>> 8];
        }

        /**
         * Returns a positive match as soon as the DFA reaches an accepting state, regardless of whether
         * the whole input has been consumed
         */
        public boolean prefixMatch(byte[] input, int offset, int length)
        {
            int state = start << 8;
            for (int i = offset; i < offset + length; i++) {
                byte inputByte = input[i];
                state = transitions[state | (inputByte & 0xFF)];

                if (state == FAIL_STATE) {
                    return false;
                }

                if (accept[state >>> 8]) {
                    return true;
                }
            }

            return accept[state >>> 8];
        }

        private static NFA makeNfa(List<Pattern> pattern, int start, int end)
        {
            checkArgument(!pattern.isEmpty(), "pattern is empty");

            NFA.Builder builder = new NFA.Builder();

            int state = builder.addStartState();

            for (int e = start; e <= end; e++) {
                Pattern item = pattern.get(e);
                if (item instanceof Pattern.Literal) {
                    Pattern.Literal literal = (Pattern.Literal) item;
                    for (byte current : literal.getValue().getBytes(UTF_8)) {
                        state = matchByte(builder, state, current);
                    }
                }
                else if (item instanceof Pattern.Any) {
                    Pattern.Any any = (Pattern.Any) item;
                    for (int i = 0; i < any.getLength(); i++) {
                        int next = builder.addState();
                        matchSingleUtf8(builder, state, next);
                        state = next;
                    }
                }
                else if (item instanceof Pattern.ZeroOrMore) {
                    matchSingleUtf8(builder, state, state);
                }
                else {
                    throw new UnsupportedOperationException("Not supported: " + item.getClass().getName());
                }
            }

            builder.setAccept(state);

            return builder.build();
        }

        private static int matchByte(NFA.Builder builder, int state, byte value)
        {
            int next = builder.addState();
            builder.addTransition(state, new NFA.Value(value), next);
            return next;
        }

        private static void matchSingleUtf8(NFA.Builder builder, int from, int to)
        {
            /*
            Implements a state machine to recognize UTF-8 characters.

                  11110xxx       10xxxxxx       10xxxxxx       10xxxxxx
              O ───────────► O ───────────► O ───────────► O ───────────► O
              │                             ▲              ▲              ▲
              ├─────────────────────────────┘              │              │
              │          1110xxxx                          │              │
              │                                            │              │
              ├────────────────────────────────────────────┘              │
              │                   110xxxxx                                │
              │                                                           │
              └───────────────────────────────────────────────────────────┘
                                        0xxxxxxx
            */

            builder.addTransition(from, new NFA.Prefix(0, 1), to);

            int state1 = builder.addState();
            int state2 = builder.addState();
            int state3 = builder.addState();

            builder.addTransition(from, new NFA.Prefix(0b11110, 5), state1);
            builder.addTransition(from, new NFA.Prefix(0b1110, 4), state2);
            builder.addTransition(from, new NFA.Prefix(0b110, 3), state3);

            builder.addTransition(state1, new NFA.Prefix(0b10, 2), state2);
            builder.addTransition(state2, new NFA.Prefix(0b10, 2), state3);
            builder.addTransition(state3, new NFA.Prefix(0b10, 2), to);
        }
    }
}
