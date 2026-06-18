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
package com.facebook.presto.sql.parser;

import com.facebook.airlift.log.Logger;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.NotSetTransition;
import org.antlr.v4.runtime.atn.RuleStopState;
import org.antlr.v4.runtime.atn.RuleTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.antlr.v4.runtime.atn.WildcardTransition;
import org.antlr.v4.runtime.misc.IntervalSet;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.antlr.v4.runtime.atn.ATNState.BLOCK_START;
import static org.antlr.v4.runtime.atn.ATNState.RULE_START;

class ErrorHandler
        extends BaseErrorListener
{
    private static final Logger LOG = Logger.get(ErrorHandler.class);

    private final Map<Integer, String> specialRules;
    private final Map<Integer, String> specialTokens;
    private final Set<Integer> ignoredRules;

    private ErrorHandler(Map<Integer, String> specialRules, Map<Integer, String> specialTokens, Set<Integer> ignoredRules)
    {
        this.specialRules = new HashMap<>(specialRules);
        this.specialTokens = specialTokens;
        this.ignoredRules = new HashSet<>(ignoredRules);
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
    {
        try {
            Parser parser = (Parser) recognizer;

            ATN atn = parser.getATN();

            ATNState currentState;
            Token currentToken;
            RuleContext context;

            if (e != null) {
                currentState = atn.states.get(e.getOffendingState());
                currentToken = e.getOffendingToken();
                context = e.getCtx();

                if (e instanceof NoViableAltException) {
                    currentToken = ((NoViableAltException) e).getStartToken();
                }
            }
            else {
                currentState = atn.states.get(parser.getState());
                currentToken = parser.getCurrentToken();
                context = parser.getContext();
            }

            Analyzer analyzer = new Analyzer(atn, parser.getVocabulary(), specialRules, specialTokens, ignoredRules, parser.getTokenStream());
            Multimap<Integer, String> candidates = analyzer.process(currentState, currentToken.getTokenIndex(), context);

            // pick the candidate tokens associated largest token index processed (i.e., the path that consumed the most input)
            String expected = candidates.asMap().entrySet().stream()
                    .max(Comparator.comparing(Map.Entry::getKey))
                    .get()
                    .getValue().stream()
                    .sorted()
                    .collect(Collectors.joining(", "));

            message = String.format("mismatched input '%s'. Expecting: %s", ((Token) offendingSymbol).getText(), expected);
        }
        catch (Exception exception) {
            LOG.error(exception, "Unexpected failure when handling parsing error. This is likely a bug in the implementation");
        }

        throw new ParsingException(message, e, line, charPositionInLine);
    }

    private static class ParsingState
    {
        public final ATNState state;
        public final int tokenIndex;
        private final CallerContext caller;

        public ParsingState(ATNState state, int tokenIndex, CallerContext caller)
        {
            this.state = state;
            this.tokenIndex = tokenIndex;
            this.caller = caller;
        }
    }

    private static class CallerContext
    {
        public final ATNState followState;
        public final CallerContext parent;

        public CallerContext(CallerContext parent, ATNState followState)
        {
            this.parent = parent;
            this.followState = followState;
        }
    }

    private static class Analyzer
    {
        private final ATN atn;
        private final Vocabulary vocabulary;
        private final Map<Integer, String> specialRules;
        private final Map<Integer, String> specialTokens;
        private final Set<Integer> ignoredRules;
        private final TokenStream stream;

        public Analyzer(
                ATN atn,
                Vocabulary vocabulary,
                Map<Integer, String> specialRules,
                Map<Integer, String> specialTokens,
                Set<Integer> ignoredRules,
                TokenStream stream)
        {
            this.stream = stream;
            this.atn = atn;
            this.vocabulary = vocabulary;
            this.specialRules = specialRules;
            this.specialTokens = specialTokens;
            this.ignoredRules = ignoredRules;
        }

        public Multimap<Integer, String> process(ATNState currentState, int tokenIndex, RuleContext context)
        {
            return process(new ParsingState(currentState, tokenIndex, makeCallStack(context)));
        }

        private Multimap<Integer, String> process(ParsingState start)
        {
            Multimap<Integer, String> candidates = HashMultimap.create();

            // Simulates the ATN by consuming input tokens and walking transitions.
            // The ATN can be in multiple states (similar to an NFA)
            Queue<ParsingState> activeStates = new ArrayDeque<>();
            activeStates.add(start);

            while (!activeStates.isEmpty()) {
                ParsingState current = activeStates.poll();

                ATNState state = current.state;
                int tokenIndex = current.tokenIndex;
                CallerContext caller = current.caller;

                if (state.getStateType() == BLOCK_START || state.getStateType() == RULE_START) {
                    int rule = state.ruleIndex;

                    if (specialRules.containsKey(rule)) {
                        candidates.put(tokenIndex, specialRules.get(rule));
                        continue;
                    }
                    else if (ignoredRules.contains(rule)) {
                        continue;
                    }
                }

                if (state instanceof RuleStopState) {
                    if (caller != null) {
                        // continue from the target state of the rule transition in the parent rule
                        activeStates.add(new ParsingState(caller.followState, tokenIndex, caller.parent));
                    }
                    else {
                        // we've reached the end of the top-level rule, so the only candidate left is EOF at this point
                        candidates.putAll(tokenIndex, getTokenNames(IntervalSet.of(Token.EOF)));
                    }
                    continue;
                }

                for (int i = 0; i < state.getNumberOfTransitions(); i++) {
                    Transition transition = state.transition(i);

                    if (transition instanceof RuleTransition) {
                        activeStates.add(new ParsingState(transition.target, tokenIndex, new CallerContext(caller, ((RuleTransition) transition).followState)));
                    }
                    else if (transition.isEpsilon()) {
                        activeStates.add(new ParsingState(transition.target, tokenIndex, caller));
                    }
                    else if (transition instanceof WildcardTransition) {
                        throw new UnsupportedOperationException("not yet implemented: wildcard transition");
                    }
                    else {
                        IntervalSet labels = transition.label();

                        if (transition instanceof NotSetTransition) {
                            labels = labels.complement(IntervalSet.of(Token.MIN_USER_TOKEN_TYPE, atn.maxTokenType));
                        }

                        int currentToken = stream.get(tokenIndex).getType();
                        if (labels.contains(currentToken)) {
                            activeStates.add(new ParsingState(transition.target, tokenIndex + 1, caller));
                        }
                        else {
                            candidates.putAll(tokenIndex, getTokenNames(labels));
                        }
                    }
                }
            }

            return candidates;
        }

        private Set<String> getTokenNames(IntervalSet tokens)
        {
            return tokens.toSet().stream()
                    .map(token -> {
                        if (token == Token.EOF) {
                            return "<EOF>";
                        }
                        return specialTokens.getOrDefault(token, vocabulary.getDisplayName(token));
                    })
                    .collect(Collectors.toSet());
        }

        private CallerContext makeCallStack(RuleContext context)
        {
            if (context == null || context.invokingState == -1) {
                return null;
            }

            CallerContext parent = makeCallStack(context.parent);

            ATNState followState = ((RuleTransition) atn.states.get(context.invokingState).transition(0)).followState;
            return new CallerContext(parent, followState);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final Map<Integer, String> specialRules = new HashMap<>();
        private final Map<Integer, String> specialTokens = new HashMap<>();
        private final Set<Integer> ignoredRules = new HashSet<>();

        public Builder specialRule(int ruleId, String name)
        {
            specialRules.put(ruleId, name);
            return this;
        }

        public Builder specialToken(int tokenId, String name)
        {
            specialTokens.put(tokenId, name);
            return this;
        }

        public Builder ignoredRule(int ruleId)
        {
            ignoredRules.add(ruleId);
            return this;
        }

        public ErrorHandler build()
        {
            return new ErrorHandler(specialRules, specialTokens, ignoredRules);
        }
    }
}
