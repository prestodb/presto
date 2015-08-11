=========
Functions
=========

First, if you have not already looked at :doc:`/develop/spi-overview`, check there for
general plugin development information.

Plugin Implementation
---------------------
The function framework is used to implement SQL functions. Presto includes a
number of built-in functions. In order to implement new functions, you can
write a plugin that has a dependency on ``presto-main`` -- also known as an "internal
"plugin" -- which returns a ``FunctionFactory`` from ``getServices()``. For example:

.. code-block:: java

    import com.facebook.presto.metadata.FunctionFactory;
    import com.facebook.presto.spi.Plugin;
    import com.facebook.presto.spi.type.TypeManager;
    import com.google.common.collect.ImmutableList;

    import javax.inject.Inject;

    import java.util.List;

    import static java.util.Objects.requireNonNull;

    public class ExampleFunctionsPlugin
            implements Plugin
    {
        private TypeManager typeManager;

        @Inject
        public void setTypeManager(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }


        @Override
        public <T> List<T> getServices(Class<T> type)
        {
            if (type == FunctionFactory.class) {
                return ImmutableList.of(type.cast(new ExampleFunctionFactory(typeManager))));
            }
            return ImmutableList.of();
        }
    }


Note that the ``ImmutableList`` class is a utility class from Guava. The
``TypeManager`` is necessary for the ``FunctionFactory`` to build functions properly,
and is injected using dependency injection.

The ``FunctionFactory`` should return a ``List<ParametricFunction>`` which contains
all of the functions defined in a given plugin. Continuing with our previous example:

.. code-block:: java

    import com.facebook.presto.metadata.FunctionFactory;
    import com.facebook.presto.metadata.FunctionListBuilder;
    import com.facebook.presto.metadata.ParametricFunction;
    import com.facebook.presto.spi.type.TypeManager;

    import java.util.List;

    public class ExampleFunctionFactory
            implements FunctionFactory
    {
        private final TypeManager typeManager;

        public ExampleFunctionFactory(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        @Override
        public List<ParametricFunction> listFunctions()
        {
            return new FunctionListBuilder(typeManager)
                    .scalar(ExampleNullFunction.class)
                    .scalar(ExampleStringFunction.class)
                    .aggregate(ExampleAverageFunction.class)
                    .getFunctions();
        }
    }


``listFunctions()`` contains all of the functions that we will implement below
in this tutorial.

For a full example in the codebase, see either the ``presto-ml`` directory for machine
learning functions or the ``presto-teradata-functions`` directory for Teradata-compatible
functions, both in the root of the Presto source.

Scalar Function Implementation
------------------------------
The function framework uses annotations to indicate relevant information
about functions, including name, description, return type, and parameter
types. Below is a sample function which implements ``is_null``:

.. code-block:: java

    import com.facebook.presto.operator.Description;
    import com.facebook.presto.operator.scalar.ScalarFunction;
    import com.facebook.presto.spi.type.StandardTypes;
    import com.facebook.presto.type.SqlType;
    import io.airlift.slice.Slice;

    import javax.annotation.Nullable;

    public class ExampleNullFunction
    {
        @ScalarFunction("is_null")
        @Description("Returns TRUE if the argument is NULL")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isNull(@Nullable @SqlType(StandardTypes.VARCHAR) Slice string)
        {
            return (string == null);
        }
    }


The new function ``is_null`` takes a single ``VARCHAR`` argument and returns a
``BOOLEAN`` indicating if the argument was ``NULL``. Note that the argument to
the function is of type ``Slice``. ``VARCHAR`` uses ``Slice``, which is essentially
a wrapper around ``byte[]``, rather than ``String`` for its native container type.

* ``@SqlType``:

  The ``@SqlType`` annotation is used to declare the return type and the argument
  types. Note that the return type and arguments of the Java code must match
  the native container types of the corresponding annotations.

* ``@Nullable``:

  The ``@Nullable`` annotation indicates that the argument may be ``NULL``. Without
  this annotation, the framework assumes that all functions return ``NULL`` if
  any of their arguments are ``NULL``. When working with a ``Type`` that has a
  primitive native container type, such as ``BigintType``, use the object wrapper for the
  native container type when using ``@Nullable``. If a method can return ``NULL`` when
  the arguments are non-null, it must be annotated with ``@Nullable``.

Another Scalar Function Example
-------------------------------
Now for a scalar function that performs an operation on its argument: ``lowercaser``.
The ``lowercaser`` function takes a single ``VARCHAR`` argument and returns a
``VARCHAR``, which is the argument in lower case. The function is below:

.. code-block:: java

    import com.facebook.presto.operator.Description;
    import com.facebook.presto.operator.scalar.ScalarFunction;
    import com.facebook.presto.spi.type.StandardTypes;
    import com.facebook.presto.type.SqlType;
    import io.airlift.slice.Slice;
    import io.airlift.slice.Slices;

    public class ExampleStringFunction
    {
        @ScalarFunction("lowercaser")
        @Description("converts the string to alternating case")
        @SqlType(StandardTypes.VARCHAR)
        public static Slice lowercaser(@SqlType(StandardTypes.VARCHAR) Slice slice)
        {
            String argument = slice.toStringUtf8();
            return Slices.utf8Slice(argument.toLowerCase());
        }
    }


Note that for most common string functions, including converting a string to
lower case, the Slice library also provides implementations that work directly
on the underlying ``byte[]``, which have much better performance. This function
has no ``@Nullable`` annotations, meaning that if the argument is ``NULL``,
the result will be ``NULL``.

Note also that it is possible to put multiple functions in each class; for
the sake of clarity we did not do so in this tutorial.

Aggregation Function Implementation
-----------------------------------

Aggregation functions use a similar framework to scalar functions, but are
a bit more complex.

* ``AccumulatorState``:

  All aggregation functions accumulate input rows into a state object; this
  object must implement ``AccumulatorState``. For simple aggregations, just
  extend ``AccumulatorState`` into a new interface with the getters and setters
  you want, and the framework will generate all the implementations and
  serializers for you. If you need a more complex state object, you will need
  to implement ``AccumulatorStateFactory`` and ``AccumulatorStateSerializer``
  and provide these via the ``AccumulatorStateMetadata`` annotation. For an
  example of how to use ``AccumulatorStateMetadata``, see
  ``NumericHistogramAggregation`` in ``presto-main``.

As an example, the following code implements the aggregation function ``avg_double`` which computes the
average of a ``DOUBLE`` column.

.. code-block:: java

    import com.facebook.presto.operator.aggregation.AggregationFunction;
    import com.facebook.presto.operator.aggregation.CombineFunction;
    import com.facebook.presto.operator.aggregation.InputFunction;
    import com.facebook.presto.operator.aggregation.OutputFunction;
    import com.facebook.presto.operator.aggregation.state.LongAndDoubleState;
    import com.facebook.presto.spi.block.BlockBuilder;
    import com.facebook.presto.spi.type.StandardTypes;
    import com.facebook.presto.type.SqlType;

    import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

    @AggregationFunction("avg_double")
    public class AverageAggregation
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


The average has two parts: the sum of the ``DOUBLE`` in each row of the column
and the ``LONG`` count of the number of rows seen. ``LongAndDoubleState`` is an interface
which extends ``AccumulatorState``, which can be found in the Presto codebase:

.. code-block:: java

    public interface LongAndDoubleState
            extends AccumulatorState
    {
        long getLong();

        void setLong(long value);

        double getDouble();

        void setDouble(double value);
    }

As stated above, for simple ``AccumulatorState`` objects, it is sufficient
just to define the interface with the getters and setters, and the framework
will generate the implementation for you.

An in-depth look at the various annotations relevant to writing an aggregation
function follows.

* ``@InputFunction``:

  The ``@InputFunction`` annotation declares the function which accepts input
  rows and stores them in the ``AccumulatorState``. Similar to scalar functions
  you must annotate the arguments with ``@SqlType``.  Note that, unlike in the above
  scalar example where ``Slice`` is used to hold ``VARCHAR``, simply the primitive
  ``double`` type is used for the argument to input. In this example, the input
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

* Where does serialization happen, and what is ``GroupedAccumulatorState``?

  The ``@InputFunction`` is usually run on a different worker from the
  ``@CombineFunction``, so the state objects are serialized and transported
  between these workers by the aggregation framework. ``GroupedAccumulatorState``
  is used when performing a ``GROUP BY`` aggregation, and an implementation
  will be automatically generated for you, if you don't specify a
  ``AccumulatorStateFactory``