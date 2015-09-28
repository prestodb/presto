=========
Functions
=========

The function framework is used to implement SQL functions. Presto includes a
number of built-in functions, and an internal ``Plugin`` (plugins that have a
dependency on presto-main) can provide new functions by returning a
``FunctionFactory`` from ``getServices()``.

.. code-block:: java

    @ScalarFunction("is_null")
    @Description("Returns TRUE if the argument is NULL")
    @SqlType(StandardTypes.Boolean)
    public static boolean isNull(@Nullable @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        return (string == null);
    }

The above code implements a new function ``is_null`` which takes a single ``VARCHAR``
argument, and returns a ``BOOLEAN`` indicating if the argument was ``NULL``.
Note that the argument to the function is of type ``Slice``. ``VARCHAR`` uses
``Slice``, which is essentially a wrapper around ``byte[]``, rather than ``String``
for its native container type.

* ``@SqlType``:

  The ``@SqlType`` annotation is used to declare the return type and the argument
  types. Note, that the return type and arguments of the Java code, must match
  the native container types of the corresponding annotations.

* ``@Nullable``:

  The ``@Nullable`` annotation indicates that the argument may be ``NULL``. Without
  this annotation the framework assumes that all functions return ``NULL`` if
  any of their arguments are ``NULL``. When working with a ``Type`` that has a
  primitive native container type, such as ``BigintType``, use the object wrapper for the
  native container type when using ``@Nullable``. The method must be annotated with
  ``@Nullable`` if it can return ``NULL`` when the arguments are non-null.

Parametric Scalar Functions
---------------------------

Scalar functions that have type parameters have some additional complexity.
To make our previous example work with any type we need the following:

.. code-block:: java

    @ScalarFunction(name = "is_null")
    @Description("Returns TRUE if the argument is NULL")
    public final class IsNullFunction
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isNullSlice(@Nullable @SqlType("T") Slice value)
        {
            return (value == null);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isNullLong(@Nullable @SqlType("T") Long value)
        {
            return (value == null);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isNullDouble(@Nullable @SqlType("T") Double value)
        {
            return (value == null);
        }

        // ...and so on for each native container type
    }

* ``@TypeParameter``:

  The ``@TypeParameter`` annotation is used to declare a type parameter which can
  be used in the argument types ``@SqlType`` annotation, or return type of the function.
  It can also be used to annotate a parameter of type ``Type``. At runtime, the engine
  will bind the concrete type to this parameter. ``@OperatorDependency`` may be used
  to declare that an additional function for operating on the given type parameter is needed.
  For example, the following function will only bind to types which have an equals function
  defined:

.. code-block:: java

    @ScalarFunction(name = "is_equal_or_null")
    @Description("Returns TRUE if arguments are equal or both NULL")
    public final class IsEqualOrNullFunction
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isEqualOrNullSlice(
                @OperatorDependency(operator = OperatorType.EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
                @Nullable @SqlType("T") Slice value1,
                @Nullable @SqlType("T") Slice value2)
        {
            if (value1 == null && value2 == null) {
                return true;
            }
            if (value1 == null || value2 == null) {
                return false;
            }
            return (boolean) equals.invokeExact(value1, value2);
        }

        // ...and so on for each native container type
    }

Aggregation Functions
---------------------

Aggregation functions use a similar framework to scalar functions, but involve
a bit more complexity.

* ``AccumulatorState``:

  All aggregation functions accumulate input rows into a state object; this
  object must implement ``AccumulatorState``. For simple aggregations, just
  extend ``AccumulatorState`` into a new interface with the getters and setters
  you want, and the framework will generate all the implementations and
  serializers for you. If you need a more complex state object, you will need
  to implement ``AccumulatorStateFactory`` and ``AccumulatorStateSerializer``
  and provide these via the ``AccumulatorStateMetadata`` annotation.

.. code-block:: java

    @AggregationFunction("avg")
    public final class AverageAggregation
    {
        @InputFunction
        public static void input(LongAndDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
        {
            state.setLong(state.getLong() + 1);
            state.setDouble(state.getDouble() + value);
        }

        @CombineFunction
        public static void combine(LongAndDoubleState state, LongAndDoubleState otherState)
        {
            state.setLong(state.getLong() + otherState.getLong());
            state.setDouble(state.getDouble() + otherState.getDouble());
        }

        @OutputFunction(StandardTypes.DOUBLE)
        public static void output(LongAndDoubleState state, BlockBuilder out)
        {
            long count = state.getLong();
            if (count == 0) {
                out.appendNull();
            }
            else {
                double value = state.getDouble();
                DOUBLE.writeDouble(out, value / count);
            }
        }
    }

The above code implements the aggregation function ``avg`` which computes the
average of a ``DOUBLE`` column.

* ``@InputFunction``:

  The ``@InputFunction`` annotation declares the function which accepts input
  rows and stores them in the ``AccumulatorState``. Similar to scalar functions
  you must annotate the arguments with ``@SqlType``. In this example, the input
  function simply keeps track of the running count of rows (via ``setLong()``)
  and the running sum (via ``setDouble()``).

* ``@CombineFunction``:

  The ``@CombineFunction`` annotation declares the function used to combine two
  state objects. This function is used to merge all the partial aggregation states.
  It takes two state objects, and merges the results into the first one (in the
  above example, just by adding them together).

* ``@OutputFunction``:

  The ``@OutputFunction`` is the last function called when computing an
  aggregation. It takes the final state object (the result of merging all
  partial states) and writes the result to a ``BlockBuilder``.

* Where does serialization happen, and what is ``@GroupedAccumulatorState``?

  The ``@InputFunction`` is usually run on a different worker from the
  ``@CombineFunction``, so the state objects are serialized and transported
  between these workers by the aggregation framework. ``@GroupedAccumulatorState``
  is used when performing a ``GROUP BY`` aggregation, and an implementation
  will be automatically generated for you, if you don't specify a
  ``AccumulatorStateFactory``
