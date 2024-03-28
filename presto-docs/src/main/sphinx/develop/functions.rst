=========
Functions
=========

Plugin Implementation
---------------------

The function framework is used to implement SQL functions. Presto includes a
number of built-in functions. In order to implement new functions, you can
write a plugin that returns one more functions from ``getFunctions()``:

.. code-block:: java

    public class ExampleFunctionsPlugin
            implements Plugin
    {
        @Override
        public Set<Class<?>> getFunctions()
        {
            return ImmutableSet.<Class<?>>builder()
                    .add(ExampleNullFunction.class)
                    .add(IsNullFunction.class)
                    .add(IsEqualOrNullFunction.class)
                    .add(ExampleStringFunction.class)
                    .add(ExampleAverageFunction.class)
                    .build();
        }
    }

Note that the ``ImmutableSet`` class is a utility class from Guava.
The ``getFunctions()`` method contains all of the classes for the functions
that we will implement below in this tutorial.

For a full example in the codebase, see either the ``presto-ml`` module for machine
learning functions or the ``presto-teradata-functions`` module for Teradata-compatible
functions, both in the root of the Presto source.

Scalar Function Implementation
------------------------------

The function framework uses annotations to indicate relevant information
about functions, including name, description, return type and parameter
types. Below is a sample function which implements ``is_null``:

.. code-block:: java

    public class ExampleNullFunction
    {
        @ScalarFunction("is_null", calledOnNullInput = true)
        @Description("Returns TRUE if the argument is NULL")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isNull(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice string)
        {
            return (string == null);
        }
    }

The function ``is_null`` takes a single ``VARCHAR`` argument and returns a
``BOOLEAN`` indicating if the argument was ``NULL``. Note that the argument to
the function is of type ``Slice``. ``VARCHAR`` uses ``Slice``, which is essentially
a wrapper around ``byte[]``, rather than ``String`` for its native container type.

* ``@SqlType``:

  The ``@SqlType`` annotation is used to declare the return type and the argument
  types. Note that the return type and arguments of the Java code must match
  the native container types of the corresponding annotations.

* ``@SqlNullable``:

  The ``@SqlNullable`` annotation indicates that the argument may be ``NULL``. Without
  this annotation the framework assumes that all functions return ``NULL`` if
  any of their arguments are ``NULL``. When working with a ``Type`` that has a
  primitive native container type, such as ``BigintType``, use the object wrapper for the
  native container type when using ``@SqlNullable``. The method must be annotated with
  ``@SqlNullable`` if it can return ``NULL`` when the arguments are non-null.

Parametric Scalar Functions
---------------------------

Scalar functions that have type parameters have some additional complexity.
To make our previous example work with any type we need the following:

.. code-block:: java

    @ScalarFunction(name = "is_null", calledOnNullInput = true)
    @Description("Returns TRUE if the argument is NULL")
    public final class IsNullFunction
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isNullSlice(@SqlNullable @SqlType("T") Slice value)
        {
            return (value == null);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isNullLong(@SqlNullable @SqlType("T") Long value)
        {
            return (value == null);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isNullDouble(@SqlNullable @SqlType("T") Double value)
        {
            return (value == null);
        }

        // ...and so on for each native container type
    }

* ``@TypeParameter``:

  The ``@TypeParameter`` annotation is used to declare a type parameter which can
  be used in the argument types ``@SqlType`` annotation, or return type of the function.
  It can also be used to annotate a parameter of type ``Type``. At runtime, the engine
  will bind the concrete type to this parameter. Optionally, the type parameter
  can be constrained to descendants of a particular type by providing a ``boundedBy``
  type class to ``@TypeParameter``.
  ``@OperatorDependency`` may be used to declare that an additional function
  for operating on the given type parameter is needed.
  For example, the following function will only bind to types which have an equals function
  defined:

.. code-block:: java

    @ScalarFunction(name = "is_equal_or_null", calledOnNullInput = true)
    @Description("Returns TRUE if arguments are equal or both NULL")
    public final class IsEqualOrNullFunction
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isEqualOrNullSlice(
                @OperatorDependency(operator = OperatorType.EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle equals,
                @SqlNullable @SqlType("T") Slice value1,
                @SqlNullable @SqlType("T") Slice value2)
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

Another Scalar Function Example
-------------------------------

The ``lowercaser`` function takes a single ``VARCHAR`` argument and returns a
``VARCHAR``, which is the argument converted to lower case:

.. code-block:: java

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
has no ``@SqlNullable`` annotations, meaning that if the argument is ``NULL``,
the result will automatically be ``NULL`` (the function will not be called).

Codegen Scalar Function Implementation
--------------------------------------

Scalar functions can also be implemented in bytecode, allowing us to specialize
and optimize functions according to the ``@TypeParameter``

* ``@CodegenScalarFunction``:

  The ``@CodegenScalarFunction`` annotation is used to declare a scalar function
  which is implemented in bytecode. ``@SqlType`` annotation is used to declare the
  return type. It takes ``Type`` as parameters which have ``@SqlType`` annotation as well.
  Return type is ``MethodHandle`` which is codegen function method.

.. code-block:: java

    public class CodegenArrayLengthFunction
    {
        @CodegenScalarFunction("array_length", calledOnNullInput = true)
        @SqlType(StandardTypes.INTEGER)
        @TypeParameter("K")
        public static MethodHandle arrayLength(@SqlType("array(K)") Type arr)
        {
            CallSiteBinder binder = new CallSiteBinder();
            ClassDefinition classDefinition = new ClassDefinition(a(Access.PUBLIC, FINAL), makeClassName("ArrayLength"), type(Object.class));
            classDefinition.declareDefaultConstructor(a(PRIVATE));

            Parameter inputBlock = arg("inputBlock", Block.class);
            MethodDefinition method = classDefinition.declareMethod(a(Access.PUBLIC, STATIC), "array_length", type(Block.class), ImmutableList.of(inputBlock));
            BytecodeBlock body = method.getBody();
            body.append(inputBlock.invoke("getPositionCount", int.class).ret());

            Class<?> clazz = defineClass(classDefinition, Object.class, binder.getBindings(), CodegenArrayLengthFunction.class.getClassLoader());
            return new methodHandle(clazz, "array_length", Block.class), Optional.of();
        }
    }

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
  and provide these via the ``AccumulatorStateMetadata`` annotation.

The following code implements the aggregation function ``avg_double`` which computes the
average of a ``DOUBLE`` column:

.. code-block:: java

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
which extends ``AccumulatorState``:

.. code-block:: java

    public interface LongAndDoubleState
            extends AccumulatorState
    {
        long getLong();

        void setLong(long value);

        double getDouble();

        void setDouble(double value);
    }

As stated above, for simple ``AccumulatorState`` objects, it is sufficient to
just to define the interface with the getters and setters, and the framework
will generate the implementation for you.

An in-depth look at the various annotations relevant to writing an aggregation
function follows:

* ``@InputFunction``:

  The ``@InputFunction`` annotation declares the function which accepts input
  rows and stores them in the ``AccumulatorState``. Similar to scalar functions
  you must annotate the arguments with ``@SqlType``.  Note that, unlike in the above
  scalar example where ``Slice`` is used to hold ``VARCHAR``, the primitive
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


Advanced Use Cases
------------------

Raw Block Inputs
^^^^^^^^^^^^^^^^

Both scalar and aggregation function annotations allow you to define methods
which operate on native types. In Java, these native types are ``boolean``,
``Slice``, and ``long``. For parameterized implementations or parametric types,
the standard Java types can't be used as they aren't able to represent the input
data.

To define a method handle which can accept *any* types, use ``@BlockPosition``
in conjunction with the ``@BlockIndex`` parameters. Similar to the
``@SqlNullable`` annotation, use the ``@NullablePosition`` annotation to denote
that the function should be called when the block position is ``NULL``.

This works for both scalar and aggregation function implementations.

.. code-block:: java

    @ScalarFunction("example")
    public static Block exampleFunction(
            @BlockPosition @NullablePosition @SqlType("array(int)") Block block,
            @BlockIndex int index) { /* ...implementation */ }

Applying Generic Types with ``@BlockPosition``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Function signatures that use the ``@BlockPosition`` syntax are able to operate
over generic types when the function is defined with a ``@TypeParameter``
annotation. Augment the ``@BlockPosition`` argument with an additional
``@SqlType("T")`` annotation to denote that it accepts an argument corresponding
to the generic type. This works for both scalar and aggregation function
implementations.

.. code-block:: java

    @ScalarFunction("example")
    @TypeParameter("T")
    public static Block exampleFunction(
            @BlockPosition @SqlType("T") Block block,
            @BlockIndex int index) { /* ...implementation */ }


Retrieving the Generic Type with ``@TypeParameter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add the ``@TypeParameter`` annotation at the beginning of a function's argument
list to allow the implementation to perform type-specific logic. Add a
``Type``-typed argument annotated with ``@TypeParameter`` as the first argument
of the function signature to get access to the ``Type``. This works for both
scalar and aggregation functions.

.. code-block:: java
    @ScalarFunction("example")
    @TypeParameter("T")
    public static Block exampleFunction(
            @TypeParameter("T") Type type,
            @BlockPosition @SqlType("T") Block block,
            @BlockIndex int index) { /* ...implementation */ }

Complex Type Function Descriptor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Complex type function descriptor is used for specifying the metadata for the map
or array functions that deal with arguments of ``ROW`` types. The Presto optimizer
tries to collect information about accessed subfields as granular as possible
so the file format reader (ORC/DWRF/Parquet) can prune all subfields that are
irrelevant for execution of the current query. Consider the example below.

.. code-block:: sql

    CREATE TABLE my_table (array_of_structs array(row(a bigint, b varchar, c double, d row(d1 bigint, d2 double))))
    SELECT CARDINALITY(FILTER(array_of_structs, x -> x.a > 0)) FROM my_table
    SELECT FILTER(array_of_structs, x -> x.a > 0) FROM my_table

In the query ``SELECT CARDINALITY(FILTER(array_of_structs, x -> x.a > 0)) FROM my_table``,
we need only the ``array_of_structs[*].x`` subfield to execute the query. However,
in the query ``SELECT FILTER(array_of_structs, x -> x.a > 0) FROM my_table`` we need
all of the subfields of column ``array_of_structs``, due to the difference in
intrinsic properties of these functions. Complex type function descriptor captures
those properties.

The complex type function descriptor could be defined for ``@ScalarFunction`` and
``@CodegenScalarFunction`` annotation using ``descriptor`` parameter.

.. code-block:: java

    @Description("Returns true if the array contains one or more elements that match the given predicate")
    @ScalarFunction(value = "any_match", descriptor = @ScalarFunctionDescriptor(
            isAccessingInputValues = true,
            argumentIndicesContainingMapOrArray = {},
            outputToInputTransformationFunction = "clearRequiredSubfields",
            lambdaDescriptors = {
                        @ScalarFunctionLambdaDescriptor(
                            lambdaArgumentDescriptors = {
                                    @ScalarFunctionLambdaArgumentDescriptor(callArgumentIndex = 0)})}))
    public final class ArrayAnyMatchFunction
    {
        ...
    }

* ``isAccessingInputValues``:
  Indicates whether the function accesses subfields. If the function accesses
  the subfields internally, then the Presto optimizer checks
  ``lambdaDescriptors`` to collect all accessed subfields. If ``lambdaDescriptors``
  is empty, then it disqualifies the function from the optimization. In this
  case, the Presto optimizer assumes that all subfields are required.
  Default value is ``true``.

* ``lambdaDescriptors``:
  Contains the array of ``@ScalarFunctionLambdaDescriptor`` for each lambda
  that this function accepts. ``@ScalarFunctionLambdaDescriptor`` contains only
  one property:

  * ``lambdaArgumentDescriptors``:
    Contains the array of ``@ScalarFunctionLambdaArgumentDescriptor``
    for each argument that lambda accepts. ``@ScalarFunctionLambdaArgumentDescriptor``
    contains two properties:

    * ``@callArgumentIndex``:
      Index of the function argument that contains the function input (Array or Map),
      to which this lambda argument relate to.
      Value ``-1`` has a special meaning to ignore subfields from this argument.

      Example 1: ``SELECT ANY_MATCH(array_of_structs, x -> x.a > 0) FROM my_table``.
      The lambda passed to the ``ANY_MATCH`` function is invoked internally in ``ANY_MATCH``
      function passing the element of the array, which ``ANY_MATCH`` receives in the
      first argument (with index 0 in a 0-indexed array of function arguments) of
      the ``ANY_MATCH`` function. Therefore, ``@callArgumentIndex`` of the
      ``@ScalarFunctionLambdaArgumentDescriptor`` for the  ``ANY_MATCH`` function should be ``0``.

      Example 2: ``SELECT MAP_FILTER(map_struct_to_struct, (key, value) -> key.x > 0 && value.y > 0) FROM my_table``.
      The lambda passed to the ``MAP_FILTER`` function is invoked internally in
      ``MAP_FILTER`` function passing the map entry, which ``MAP_FILTER``
      receives in the first argument (with index 0 in a 0-indexed array of
      function arguments) of the ``MAP_FILTER`` function. Currently,
      Presto does not support subfield pruning for map keys. Therefore, we
      must ignore subfield ``x`` by setting ``@callArgumentIndex`` to ``-1``.
      However, Presto supports subfield pruning for map values.
      Therefore, for the ``value`` argument we should set
      ``@callArgumentIndex`` in the ``@ScalarFunctionLambdaArgumentDescriptor``
      for the  ``MAP_FILTER`` to ``0``.

    * ``@lambdaArgumentToInputTransformationFunction``:
      Contains the subfield path transformation function between the subfields
      of this lambda argument and the input of the function. See also
      pre-defined list of subfield path transformation functions in the description
      of the ``outputToInputTransformationFunction`` parameter of the ``@ScalarFunction`` and
      ``@CodegenScalarFunction`` annotation below.

      Default value for this parameter is ``"prependAllSubscripts"``,
      which adds prefix ``[*]`` to the path of the subfield,
      which is correct for 99% of the functions.

      Example: ``SELECT ANY_MATCH(array_of_structs, x -> x.a > 0) FROM my_table``.
      Even though in the lambda the access subfield has path ``a``,
      we must add the prefix ``[*]``, because ``array_of_structs`` is of array type.

  Default value of the ``lambdaDescriptors`` is an empty array. If function
  does not accept any lambda parameter, then ``lambdaDescriptors``
  should be an empty array.

* ``argumentIndicesContainingMapOrArray``:
  Consider ``TRIM_ARRAY`` function in this example:
  ``SELECT ANY_MATCH(TRIM_ARRAY(array_of_structs, 5), x -> x.a > 0) FROM my_table``.
  By default, the optimizer passes the subfield accessed in outer functions
  (here, subfield ``[*].a``, which is accessed in function ``ANY_MATCH``)
  to all array or map arguments
  (here, to the array ``array_of_structs``).
  Consider the function ``MAP`` in this example:

  .. code-block:: sql

     CREATE TABLE my_table (keys array(bigint), values array(row(a bigint, b varchar)))
     SELECT ANY_MATCH(MAP_VALUES(MAP(keys, values)), x -> x.a > 100)

  In ``MAP`` function, both ``keys`` and ``values`` are of array type. However,
  subfields accessed in function ``ANY_MATCH`` relate only to ``values``.
  Note, for map type, we collect only subfields of map values.  Default
  behavior does not work here and we must manually specify for ``MAP`` function
  that subfields accessed in outer functions relate only to the second argument
  (with index 1 in 0-indexed array of arguments). Therefore, for ``MAP``
  function we must specify ``argumentIndicesContainingMapOrArray = {1}``.
  If the function does not have such behaviour like ``MAP`` function,
  ``argumentIndicesContainingMapOrArray`` parameter could be omitted.

* ``outputToInputTransformationFunction``:
  Contains the subfield path transformation function to convert the output back
  to the input elements of the array or map. Optimizer examines the nested
  function calls from the most outer function to the most inner function.
  Often the path of the subfields must be altered, reverting the effect that is
  done by the function to its input. Let's consider the example:

  .. code-block:: sql

      CREATE TABLE my_table (2d_array_of_structs array(array(row(p bigint, q row(e1 bigint, e2 varchar)))))
      SELECT TRANSFORM(FLATTEN(2d_array_of_structs), x -> x.p) FROM my_table

  Here, column ``2d_array_of_structs`` is a 2-dimensional array or rows.
  Function ``FLATTEN`` produces the 1-dimensional array of rows. In
  ``TRANSFORM``, subfield ``[*].p`` is accessed. The challenge now is to
  correctly attribute accessed subfield ``[*].p`` to the original column
  ``2d_array_of_structs`` where the path of the subfield ``p`` is ``[*][*].p``.
  This is when ``outputToInputTransformationFunction`` comes into play. Because
  of the internal property of the ``FLATTEN`` function, for any subfield that
  accessed in outer calls we must transform the path of the subfield and add
  additional ``[*]`` prefix.

  There are several pre-defined subfield path transformation functions:

  * ``"identity"``:
    Identity function (no transformation needed).

  * ``"prependAllSubscripts"``:
    Adds prefix ``[*]`` (all subscripts) to the path of the subfield path.
    It is a default value for ``@lambdaArgumentToInputTransformationFunction``
    parameter in ``@ScalarFunctionLambdaArgumentDescriptor``.

  * ``"allSubfieldsRequired"``:
    This transformation function indicates that all subfields are required. At
    this point, the Presto optimizer discards any collected subfields from
    outer calls and adds the special subfield ``[*]``  (all subscripts
    without specifying any subfield), which essentially means that all
    subfields are required.
    It means that the transformation of the output to input is unknown, and
    thus the lambda subfields pushdown from outer calls could not be done.
    This is a default value of ``@ScalarFunction`` annotation.

  * ``"clearRequiredSubfields"``:
    There are some functions that do not send the input to the output. For
    instance, ``CARDINALITY`` function, does not return the input array like
    the ``FILTER`` function does. Instead, it returns only the number (the size
    of the array). Knowing this property of the function, we can safely discard
    any accessed subfields in outer functions (even ``[*]``) that essentially
    means that the Presto optimizer can start collecting the accessed subfields
    from now on. If we are lucky and any other inner call does not include
    ``"allSubfieldsRequired"``, then we can conclude that only those collected
    subfields are required for evaluating the expression and we can safely
    prune all other subfields.

  If needed, a new subfield path transformation function could be added as
  a static method in ``SubfieldPathTransformationFunctions`` class.