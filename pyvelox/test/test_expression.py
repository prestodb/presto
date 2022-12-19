# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pyvelox.pyvelox as pv
import unittest


class TestVeloxExpression(unittest.TestCase):
    def test_from_string(self):
        pv.Expression.from_string("a + b")

    def test_eval(self):
        expr = pv.Expression.from_string("a + b")
        a = pv.from_list([1, 2, 3, None])
        b = pv.from_list([4, 5, None, 6])
        c = expr.evaluate(["a", "b"], [a, b])
        self.assertEqual(c[0], 5)
        self.assertEqual(c[1], 7)
        self.assertEqual(c[2], None)
        self.assertEqual(c[3], None)

        # Ignore extra arguments
        d = pv.from_list([1, 2, 3, None])
        e = expr.evaluate(["a", "b", "d"], [a, b, d])
        self.assertEqual(e[0], 5)
        self.assertEqual(e[1], 7)
        self.assertEqual(e[2], None)
        self.assertEqual(e[3], None)

        with self.assertRaises(ValueError):
            d = pv.from_list([10, 11])
            expr.evaluate(["a", "b"], [a, d])

        with self.assertRaises(RuntimeError):
            d = pv.from_list(["hi", "bye", "hello", "goodbye"])
            expr.evaluate(["a", "b"], [a, d])

    def test_eval_map(self):
        expr = pv.Expression.from_string("a + b")
        a = pv.from_list([1, 2, 3, None])
        b = pv.from_list([4, 5, None, 6])
        c = expr.evaluate({"a": a, "b": b})
        self.assertEqual(c[0], 5)
        self.assertEqual(c[1], 7)
        self.assertEqual(c[2], None)
        self.assertEqual(c[3], None)

        # Ignore extra arguments
        d = pv.from_list([1, 2, 3, None])
        e = expr.evaluate({"a": a, "b": b, "d": d})
        self.assertEqual(e[0], 5)
        self.assertEqual(e[1], 7)
        self.assertEqual(e[2], None)
        self.assertEqual(e[3], None)

    def test_functions(self):
        concat = pv.Expression.from_string("concat(a, b)")

        a = pv.from_list(["Hello", "Happy"])
        b = pv.from_list([" world", " birthday"])

        c = concat.evaluate({"a": a, "b": b})
        self.assertEqual(c[0], "Hello world")
        self.assertEqual(c[1], "Happy birthday")

    def test_cast(self):
        cast_to_int = pv.Expression.from_string("cast(a AS bigint)")

        strs = pv.from_list(["1", "10", "100"])
        ints = cast_to_int.evaluate({"a": strs})
        self.assertEqual(ints[0], 1)
        self.assertEqual(ints[1], 10)
        self.assertEqual(ints[2], 100)

    def test_all_nulls(self):
        # A vector of all None is currently not constructable, so we hack it using an expression to return Nones
        expr = pv.Expression.from_string("a + b")
        a = pv.from_list([1, None])
        b = pv.from_list([None, 2])
        c = expr.evaluate({"a": a, "b": b})
        d = expr.evaluate({"a": a, "b": b})
        self.assertEqual(c[0], None)
        self.assertEqual(c[1], None)

        self.assertEqual(d[0], None)
        self.assertEqual(d[1], None)

        e = expr.evaluate({"a": c, "b": d})
        self.assertEqual(e[0], None)
        self.assertEqual(e[1], None)

    def test_complicated_expression(self):
        expr = pv.Expression.from_string(
            "(cast(0 - b as DOUBLE) + sqrt(cast(b * b - 4 * a * c as DOUBLE))) / cast((2 * a) as double)"
        )
        a = pv.from_list([1, 3])
        b = pv.from_list([-7, 0])
        c = pv.from_list([10, -12])

        expected = [5, 2]
        actual = expr.evaluate({"a": a, "b": b, "c": c})
        for e, a in zip(expected, actual):
            self.assertEqual(e, a)

    def test_switch_case(self):
        expr = pv.Expression.from_string(
            "case a when 1 then 'one' when 2 then 'two' else 'many' end"
        )
        a = pv.from_list([1, 2, 3, 4, 5, 1])
        b = expr.evaluate({"a": a})
        self.assertEqual(b[0], "one")
        self.assertEqual(b[1], "two")
        self.assertEqual(b[2], "many")
        self.assertEqual(b[3], "many")
        self.assertEqual(b[4], "many")
        self.assertEqual(b[5], "one")
