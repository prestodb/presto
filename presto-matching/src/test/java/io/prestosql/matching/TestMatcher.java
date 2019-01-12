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
package com.facebook.presto.matching;

import com.facebook.presto.matching.example.rel.FilterNode;
import com.facebook.presto.matching.example.rel.JoinNode;
import com.facebook.presto.matching.example.rel.ProjectNode;
import com.facebook.presto.matching.example.rel.RelNode;
import com.facebook.presto.matching.example.rel.ScanNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.matching.Pattern.any;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.matching.example.rel.Patterns.filter;
import static com.facebook.presto.matching.example.rel.Patterns.plan;
import static com.facebook.presto.matching.example.rel.Patterns.project;
import static com.facebook.presto.matching.example.rel.Patterns.scan;
import static com.facebook.presto.matching.example.rel.Patterns.source;
import static com.facebook.presto.matching.example.rel.Patterns.tableName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestMatcher
{
    @Test
    public void trivialMatchers()
    {
        //any
        assertMatch(any(), 42);
        assertMatch(any(), "John Doe");

        //class based
        assertMatch(typeOf(Integer.class), 42);
        assertMatch(typeOf(Number.class), 42);
        assertNoMatch(typeOf(Integer.class), "John Doe");

        //predicate-based
        assertMatch(typeOf(Integer.class).matching(x -> x > 0), 42);
        assertNoMatch(typeOf(Integer.class).matching(x -> x > 0), -1);
    }

    @Test
    public void matchObject()
    {
        assertMatch(project(), new ProjectNode(null));
        assertNoMatch(project(), new ScanNode("t"));
    }

    @Test
    public void propertyMatchers()
    {
        Pattern<String> aString = typeOf(String.class);
        Property<String, Integer> length = Property.property("length", String::length);
        String string = "a";

        assertMatch(aString.with(length.equalTo(1)), string);
        assertMatch(project().with(source().matching(scan())), new ProjectNode(new ScanNode("T")));
        assertMatch(aString.with(length.matching(any())), string);
        assertMatch(aString.with(length.matching(x -> x > 0)), string);
        assertMatch(aString.with(length.matching((Number x) -> x.intValue() > 0)), string);

        assertNoMatch(aString.with(length.equalTo(0)), string);
        assertNoMatch(project().with(source().matching(scan())), new ProjectNode(new ProjectNode(new ScanNode("T"))));
        assertNoMatch(aString.with(length.matching(typeOf(Void.class))), string);
        assertNoMatch(aString.with(length.matching(x -> x < 1)), string);
        assertNoMatch(aString.with(length.matching((Number x) -> x.intValue() < 1)), string);
    }

    @Test
    public void matchNestedProperties()
    {
        Pattern<ProjectNode> pattern = project().with(source().matching(scan()));

        assertMatch(pattern, new ProjectNode(new ScanNode("t")));
        assertNoMatch(pattern, new ScanNode("t"));
        //TODO this needs a custom Option type to work , or NPEs will happen.
        //Optional does not allow null values.
        //assertNoMatch(pattern, new ProjectNode(null));
        assertNoMatch(pattern, new ProjectNode(new ProjectNode(null)));
    }

    @Test
    public void matchAdditionalProperties()
    {
        String matchedValue = "A little string.";

        Pattern<String> pattern = typeOf(String.class)
                .matching(s -> s.startsWith("A"))
                .matching((CharSequence s) -> s.length() > 7);

        assertMatch(pattern, matchedValue);
    }

    @Test
    public void optionalProperties()
    {
        Property<RelNode, RelNode> onlySource = Property.optionalProperty("onlySource", node ->
                Optional.of(node.getSources())
                        .filter(sources -> sources.size() == 1)
                        .map((List<RelNode> sources) -> sources.get(0)));

        Pattern<RelNode> relNodeWithExactlyOneSource = plan()
                .with(onlySource.matching(any()));

        assertMatch(relNodeWithExactlyOneSource, new ProjectNode(new ScanNode("t")));
        assertNoMatch(relNodeWithExactlyOneSource, new ScanNode("t"));
        assertNoMatch(relNodeWithExactlyOneSource, new JoinNode(new ScanNode("t"), new ScanNode("t")));
    }

    @Test
    public void capturingMatchesInATypesafeManner()
    {
        Capture<FilterNode> filter = newCapture();
        Capture<ScanNode> scan = newCapture();
        Capture<String> name = newCapture();

        Pattern<ProjectNode> pattern = project()
                .with(source().matching(filter().capturedAs(filter)
                        .with(source().matching(scan().capturedAs(scan)
                                .with(tableName().capturedAs(name))))));

        ProjectNode tree = new ProjectNode(new FilterNode(new ScanNode("orders"), null));

        Match<ProjectNode> match = assertMatch(pattern, tree);
        //notice the concrete type despite no casts:
        FilterNode capturedFilter = match.capture(filter);
        assertEquals(tree.getSource(), capturedFilter);
        assertEquals(((FilterNode) tree.getSource()).getSource(), match.capture(scan));
        assertEquals("orders", match.capture(name));
    }

    @Test
    public void noMatchMeansNoCaptures()
    {
        Capture<Void> impossible = newCapture();
        Pattern<Void> pattern = typeOf(Void.class).capturedAs(impossible);

        Match<Void> match = DefaultMatcher.DEFAULT_MATCHER.match(pattern, 42);

        assertTrue(match.isEmpty());
        Throwable throwable = expectThrows(NoSuchElementException.class, () -> match.capture(impossible));
        assertTrue(throwable.getMessage().contains("Captures are undefined for an empty Match"));
    }

    @Test
    public void unknownCaptureIsAnError()
    {
        Pattern<?> pattern = any();
        Capture<?> unknownCapture = newCapture();

        Match<?> match = DefaultMatcher.DEFAULT_MATCHER.match(pattern, 42);

        Throwable throwable = expectThrows(NoSuchElementException.class, () -> match.capture(unknownCapture));
        assertTrue(throwable.getMessage().contains("unknown Capture"));
    }

    @Test
    public void nullNotMatchedByDefault()
    {
        assertNoMatch(any(), null);
        assertNoMatch(typeOf(Integer.class), null);
    }

    private <T> Match<T> assertMatch(Pattern<T> pattern, T expectedMatch)
    {
        return assertMatch(pattern, expectedMatch, expectedMatch);
    }

    private <T, R> Match<R> assertMatch(Pattern<R> pattern, T matchedAgainst, R expectedMatch)
    {
        Match<R> match = DefaultMatcher.DEFAULT_MATCHER.match(pattern, matchedAgainst);
        assertEquals(expectedMatch, match.value());
        return match;
    }

    private <T> void assertNoMatch(Pattern<T> pattern, Object expectedNoMatch)
    {
        Match<T> match = DefaultMatcher.DEFAULT_MATCHER.match(pattern, expectedNoMatch);
        assertEquals(Match.empty(), match);
    }
}
