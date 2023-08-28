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

class DenseDfaMatcher
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

    // Artificial state to sink all invalid matches
    private final int fail;

    private final boolean exact;

    /**
     * @param exact whether to match to the end of the input
     */
    public static DenseDfaMatcher newInstance(DFA dfa, boolean exact)
    {
        int[] transitions = new int[dfa.getStates().size() * 256];
        boolean[] accept = new boolean[dfa.getStates().size()];

        for (DFA.State state : dfa.getStates()) {
            for (DFA.Transition transition : dfa.transitions(state)) {
                transitions[state.getId() * 256 + transition.getValue()] = transition.getTarget().getId() * 256;
            }

            if (state.isAccept()) {
                accept[state.getId()] = true;
            }
        }

        return new DenseDfaMatcher(transitions, dfa.getStart().getId(), accept, 0, exact);
    }

    private DenseDfaMatcher(int[] transitions, int start, boolean[] accept, int fail, boolean exact)
    {
        this.transitions = transitions;
        this.start = start;
        this.accept = accept;
        this.fail = fail;
        this.exact = exact;
    }

    public boolean match(byte[] input, int offset, int length)
    {
        if (exact) {
            return exactMatch(input, offset, length);
        }

        return prefixMatch(input, offset, length);
    }

    /**
     * Returns a positive match when the final state after all input has been consumed is an accepting state
     */
    private boolean exactMatch(byte[] input, int offset, int length)
    {
        int state = start << 8;
        for (int i = offset; i < offset + length; i++) {
            byte inputByte = input[i];
            state = transitions[state | (inputByte & 0xFF)];

            if (state == fail) {
                return false;
            }
        }

        return accept[state >>> 8];
    }

    /**
     * Returns a positive match as soon as the DFA reaches an accepting state, regardless of whether
     * the whole input has been consumed
     */
    private boolean prefixMatch(byte[] input, int offset, int length)
    {
        int state = start << 8;
        for (int i = offset; i < offset + length; i++) {
            byte inputByte = input[i];
            state = transitions[state | (inputByte & 0xFF)];

            if (state == fail) {
                return false;
            }

            if (accept[state >>> 8]) {
                return true;
            }
        }

        return accept[state >>> 8];
    }
}