# Velox Coding Style and Best Practices

The goal of this document is to make Velox developers more productive by
promoting consistency within the codebase, and encouraging common practices
which will make the codebase easier to read, edit, maintain and debug in the
future.

## Code Formatting, Headers, and Licenses

Our Makefile contains targets to help highlight and fix format, header or
license issues. These targets are shortcuts for calling `./scripts/check.py`.

Use `make header-fix` to apply our open source license headers to new files.
Use `make format-fix` to identify and fix formatting issues using clang-format.

Formatting issues found on the changed lines in the current commit can be
displayed using `make format-check`.  These issues can be fixed by using `make
format-fix`. This command will apply formatting changes to modified lines in
the current commit.

Header issues found on the changed files in the current commit can be displayed
using `make header-check`. These issues can be fixed by using `make header-fix`.
This will apply license header updates to the files in the current commit.

An entire directory tree of files can be formatted and have license headers
added using the `tree` variant of the format commands:

```
    ./scripts/check.py format tree
    ./scripts/check.py format tree --fix

    ./scripts/check.py header tree
    ./scripts/check.py header tree --fix
```

All the available formatting commands can be displayed by using
`./scripts/check.py help`.

## C++ Style

Many aspects of C++ style will be covered by clang-format, such as spacing,
line width, indentation and ordering (for includes, using directives and etc). 

* Always ensure your code is clang-format compatible.
* If you’re working on a legacy file which is not clang-format compliant yet,
  refrain from formatting the entire file in the same PR as it pollutes your
  original PR and makes it harder to review.
  * Submit a separate diff/PR with the format-only changes.

## Naming Conventions

* Use **PascalCase** for types (classes, structs, enums, type aliases, type
  template parameters) and file names.
* Use **camelCase** for functions, member and local variables, and non-type
  template parameters.
* **camelCase_** for private and protected members variables.
* Use **snake_case** for namespace names and build targets
* Use **UPPER_SNAKE_CASE** for macros.
* Use **kPascalCase** for static constants and enumerators.


## Comments

Some good practices about code comments:

* Optimize code for the reader, not the writer. 
  * Velox is growing and starting to be used by multiple compute engines and
    teams; as a result, more time will be spent reading code than writing it.
* Overall goal: make the code more obvious and remove obscurity.
* Comments should capture information that was on the writer’s mind, but
  **couldn’t be represented as code.**
  * As such, refrain from adding obvious comments, e.g: simple getter/setter
    methods.
  * However, “obviousness” is in the reader’s mind - if a reviewer says
    something is not obvious, then it is not obvious. 
    * Consider that the audience is an experienced Software Engineer with a
      moderate knowledge of the codebase.
* What should be commented: 
  * Every File
  * Every Class
  * Every method that's not an obvious getter/setter
  * Every member variable
    * Do not simply restate the variable name. Either add a comment explaining
      the semantic meaning of that variable or do not add a comment at all.
      This is an anti-pattern:

    `// A simple counter.
    size_t count_{0};
    `

  * For functions with large bodies, a good practice is to group blocks of
    related code, and precede them with a blank line and a high-level comment
    on what the block does.

About comment style:

* Header comment for source files:
  * All source files (.h and .cpp) should have a standard header in a large
    comment block at the top; this should include the standard copyright
    notice, the license header, and a brief file description. This comment
    block should use `/* */`
* For single line comments, always use `//` (with a space separator before the
  comment).
* Comments should be full english sentences, starting with a capital letter and
  ending with a period (.).
  * Use:

    `// True if this node only sorts a portion of the final result.`

  * Instead of:

    `// true if this node only sorts a portion of the final result`

* For multi-line comments:
  * Velox will follow the doxygen comment style. 
  * For multi-line comments within actual code blocks (the ones which are not
    to be exposed as documentation, use `//` (double slashes).
  * For comments on headers for classes, functions, methods and member
    variables (the ones to be exposed as documentation), use `///` (three
    slashes) at the front of each line. 
  * Don't use old-style `/* */` comments inside code or on top-level header
    comments. It adds two additional lines and makes headers more verbose than
    they need to be.
* Special comments:
  * Use this format when something needs to be fixed in the future:

    `// TODO: Description of what needs to be fixed.`

  * Include enough context in the comment itself to make clear what will be
    done, without requiring any references from outside the code.
  * Do not include the author’s username. If required, this can always be
    retrieved from git blame.

## Asserts and CHECKs

* For assertions and other types of validation, use `VELOX_CHECK_*` macros
  * `VELOX_CHECK_*` will categorize the error to be an internal runtime error.
  * `VELOX_USER_CHECK_*` will categorize the error to be a user error.
* Use `VELOX_FAIL()` or `VELOX_USER_FAIL()` to inadvertently throw an
  exception:
  * `VELOX_FAIL("Illegal state");`
* Use `VELOX_UNREACHABLE()` when a particular branch/block should never be
  executed, such as in a switch statement with an invalid `default:` block.
* Use `VELOX_NYI()` for features or code paths that are not implemented yet.
* When comparing two values/expressions, prefer to use:
  * `VELOX_CHECK_LT(idx, children_.size());`
* Rather than:
  * `VELOX_CHECK(idx < children_.size());`
* The former will evaluate and include both expressions values in the
  exception.
* All macro also provide the following optional features:
  * Specifying error code (error code listed in
    `velox/common/base/VeloxException.h`): 
    * `VELOX_CHECK_EQ(v1[, v2[, error_code]]);`
    * `VELOX_USER_CHECK_EQ(v1[, v2[, error_code]])`
  * Appending error message:
    * `VELOX_CHECK_EQ(v1, v2, “Some error message”)`
    * `VELOX_USER_CHECK_EQ(v1, v2, “Some error message”)`
  * Error message formatting via fmt:
    * `VELOX_USER_CHECK_EQ(v1, v2, “My complex error message {} and {}”, str,
      i)`
    * Note that the values of v1 and v2 are already included in the exception
      message by default.

## Variables

* Do not declare more than one variable on a line (this helps accomplish the
  other guidelines, promotes commenting, and is good for tagging).
  * Obvious exception: for-loop initialization
* Initialize most variables at the time of declaration (unless they are class
  objects without default constructors).
  * Prefer using uniform-initialization to make initialization more consistent
    across types, e.g., prefer `size_t size{0};` over `size_t size = 0;`
* Declare your variables in the smallest scope possible.
* Declare your variables as close to the usage point as possible within the
  given scope.
* Don't group all your variables at the top of the scope -- this makes the code
  much harder to follow.
* If the variable or function parameter is a pointer or reference type, group
  the `*` or `&` with the type -- pointer-ness or reference-ness is an attribute
  of the type, not the name.
  * `int* foo;` `const Bar& bar;` NOT `int *foo;` `const Bar &bar`;
  * Beware that `int* foo, bar;` will be parsed as declaring `foo` as an `int*`
    and `bar` as an `int`. Note that multiple declaration is discouraged.
* For member variables:
  * Group member variable and methods based on their visibility (public,
    protected and private)
    * It’s ok to have multiple blocks for a given level. 
  * Most member variables should come with a short comment description and an
    empty line above that. 
  * Refrain from using public member variables whenever possible, in order to
    promote encapsulation.
    * Leverage getter/setter methods when appropriate.
    * Name getter methods after the variable, e.g: `foo()`, and setter methods
      using the "set" prefix, e.g: `setFoo()`
    * Always mark getter methods as const.
* Prefer to use value-types, `std::optional`, and `std::unique_ptr` in that
  order.
  * Value-types are conceptually the simplest and cheapest.
  * `std::optional` allows you to express “may be null” without the additional
    complexity of manual storage duration.
  * `std::unique_ptr<>` should be used for types that are not cheaply movable
    but need to transfer ownership, or which are too large to store on the
    stack. Note that most types that perform large allocations already store
    their bulk memory on-heap.

## Constants

* Always use `nullptr` if you need a constant that represents a null pointer
  (`T*` for some `T`); use `0` otherwise for a zero value.
* For large literal numbers, use ‘ to make it more readable, e.g:  `1’000’000`
  instead of `1000000`.
* For floating point literals, never omit the initial 0 before the decimal
  point (always `0.5`, not `.5`).
* File level variables and constants should be defined in an anonymous
  namespace.
* Always prefer const variables and enum to using preprocessor (#define) to
  define constant values. 
* Prefer `enum class` over `enum` for better type safety.
* As a general rule, do not use string literals without declaring a named
  constant for them.
  * The best way to make a constant string literal is to use constexpr
    `std::string_view`/`folly::StringPiece`
  * **NEVER** use `std::string` - this makes your code more prone to SIOF bugs.
  * Avoid `const char* const` and `const char*` - these are less efficient to
    convert to `std::string` later on in your program if you ever need to
    because `std::string_view`/ `folly::StringPiece` knows its size and can use
    a more efficient constructor. `std::string_view`/ `folly::StringPiece` also
    has richer interfaces and often works as a drop-in replacement to
    `std::string`.
  * Need compile-time string concatenation? You can use `folly::FixedString`
    for that.

## Macros

Do not use them unless absolutely necessary. Whenever possible, use normal
inline functions or templates instead. If you absolutely need, remember that
macro names are always upper-snake-case. Also:

* Use parentheses to sanitize complex inputs.
  * For example, `#define MUL(x, y) x * y` is incorrect for input like `MUL(2,
    1 + 1)`. The author probably meant `#define MUL(x, y) ((x) * (y))` instead.
* When making macros that have multiple statements, use this idiom: 
  * `do { stmt1; stmt2; } while (0)`
  * This allows this block to act the most like a single statement, usable in
    if/for/while even if they don't use braces and forces use of a trailing
    semicolon.

## Headers and Includes

* All header files must have a single-inclusion guard using `#pragma once`
* Included files and using declarations should all be at the top of the file,
  and ordered properly to make it easier to see what is included. 
  * Use clang-format to order your include and using directives.
* Includes should always use the full path (relative to github’s root dir).
* Whenever possible, try to forward-declare as much as possible in the .h and
  only `#include` things you need the full implementation for. 
  * For instance, if you just use a `Class*` or `Class&` in a header file,
    forward-declare `Class` instead of including the full header to minimize
    header dependencies.
* Put small private classes that will only get used in one place into a .cpp,
  inside an anonymous namespace. A common example is a custom comparator class.
* Be aware of what goes into your .h files. If possible, try to separate them
  into "Public API" .h files that may be included by external modules. These
  public .h files should contain only those functions or classes that external
  users need to access. 
* No large blocks of code should be in .h files that are widely included if
  these blocks aren't vital to all users' understanding of the interface the .h
  represents. Split these large implementation blocks into a separate `-inl.h`
  file.
  * `-inl.h` files should exist only to aid readability with the expectation
    that a user of your library should only need to read the .h to understand
    the interface. Only if someone needs to understand your implementation
    should opening the `-inl.h` be necessary (or the .cpp file, for that
    matter).

## Function Arguments

* Const
  * All objects, whenever possible, should be passed to functions as const-refs
    (`const T&`). This makes APIs much clearer as to whether it’s an input or
    output arg, reduces the need for NULL-checks (and reduces crashing bugs),
    and helps enforce good encapsulation.
    * An obvious exception is objects that are trivially copy- or
      move-constructible, like primitive types or `std::unique_ptr`. 
    * Don't pass by const reference when passing by value is both correct and
      cheaper.
  * All non-static member functions should be marked as const if calling them
    doesn't change the behavior/response of any member.
* References as function arguments.
  * For input or read-only parameters, pass by `const&`
  * For nullable or optional parameters, use higher order primitives like
    `std::optional`; raw pointers are also appropriate.
  * For output, mutable, or in/out parameters:
    * Non-const ref is the recommendation if the argument is not nullable. 
    * If it’s nullable, use a raw pointer.
  * Always use `std::optional` instead of `folly::Optional` and
    `boost::optional`. 
* Prefer `std::string_view` to `const std::string&`
  * This avoids unnecessary construction of a std::string when passing other
    types.  This often happens when string literals are passed into functions.
  * If you need a `std::string` inside the function, however, take a
    `std::string` to move the copy to the API boundary. This allows the caller
    to avoid the copy with a move.
* Almost never take an r-value reference as a function argument; take the
  argument by value instead. This usually as efficient as taking an r-value
  reference, but simpler and more flexible at the call site. For example: ```
  InPredicate::InPredicate(folly::F14HashSet<T>&& rhs) : rhs_(std::move(rhs))
  {} ```
  * is more complex than the version that takes expr by value: ```
    InPredicate::InPredicate(folly::F14HashSet<T> rhs) : rhs_(std::move(rhs))
    {} ```
  * However, the first requires either a std::move() or an explicit copy at the
    call point; the second doesn’t. The performance of the two should be almost
    identical.
* Add comments when it’s not clear which argument is intended in a function
  call.  This is particularly a problem for longer signatures with repeated
  types or constant arguments. For example, `phrobinicate(/*elements=*/{1, 2},
  /*startOffset=*/0, /*length=*/2)`
* Use the /*argName=*/value format (note the lack of spaces). Clang-tidy
  supports checking the given argument name against the declaration when this
  format is used.

## Namespaces

* All Velox code should be placed inside the `facebook::velox` namespace
* Always use nested namespace definition:
  * `namespace facebook::velox::core {` and not
  * `namespace facebook { namespace velox { namespace core {`
* Always add an inline comment at the end of the namespace definition, and surround
  it by empty lines:

```
namespace facebook::velox::exec {

myFunc();

} // namespace facebook::velox::exec
```

  * not:

```
namespace facebook::velox::exec {
myFunc();
}
```

* Use sub namespaces (e.g: facebook::velox::core) to logically group large
  chunks of related code and prevent identifier clashes.
  * Namespaces should make it easier to code, not harder. Refrain from creating
    hierarchies that are way too long.
  * Namespace don’t necessarily need to reflect the on-disk layout. 
    * This isn’t java.
* Guidelines for importing namespaces:
  * Don't EVER put `using namespace anything;` OR `using anything::anything;`
    in a header file. This pollutes the global namespace and makes it extremely
    difficult to refactor code.
  * It’s very useful to not have to fully-qualify types as it can make code
    more readable. In header files, it's unavoidable. ALWAYS fully qualify
    names in a header (e.g. it's `std::string`, not just `string`).
  * In cpp files, best practice is to add a using declaration after your list
    of `#includes` (e.g. `using tests::VectorMaker;`) and to then just write
    `VectorMaker`.
  * `using namespace std;` can lead to surprising behavior, especially when
    other modules don't follow best practices with their using statements and
    what they dump into global scope. The best defensive programming practice
    for std it to fully qualify symbols, e.g: `std::string`, std::vector
    * Do not use `using namespace std;`
  * Use `using namespace anything_else;` somewhat sparingly – the whole point
    of namespaces is obliterated when the code starts doing this prolifically.
    Frequently, `using foo::bar::BazClass;` is better.

## Type Aliases

* For types widely used together with std::shared_ptr, consider introducing
  aliases for std::shared_ptr<Xxx> using naming convention XxxPtr. In some
  cases it makes sense to alias std::shared_ptr<const Xxx>. Here are some
  examples of existing aliases: TypePtr, VectorPtr, FlatVectorPtr,
  ArrayVectorPtr, MapVectorPtr, RowVectorPtr, TypedExprPtr, PlanNodePtr.

```
using TypePtr = std::shared_ptr<const Type>;
```
* Similarly, widely used template types also benefit from shorter or clearer
  aliases.

```
using ContinueFuture = folly::SemiFuture<bool>;
using ContinuePromise = VeloxPromise<bool>;
```
