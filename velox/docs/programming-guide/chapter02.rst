==================================
Chapter 2: Flat Vectors of Strings
==================================

In :doc:`Chapter 1 </programming-guide/chapter01>` we learned how to allocate
buffers to store integer values and null flags and how to create flat vectors
from these buffers. In this chapter we will look into creating flat vectors of
strings.

Unlike integer values, string values have variable sizes. One string can be 10
characters long, another 20 characters long. In Velox, strings are represented
using StringView structs. Each StringView is 16 bytes long. 4 bytes store the
length of the string. The remaining 12 bytes store either the string itself, if
it fits, or 4-byte prefix of the string followed by 8-byte pointer to the string
located in a separate buffer.

.. code-block:: c++

    struct StringView {
        uint32_t size_;
        char prefix_[4];
        union {
         char inlined[8];
         const char* data;
        } value_;
    }

Strings that are 12 bytes or shorter are referred to as inline strings. These
strings are fully stored within the StringView struct.

Flat vector of strings has values buffer that stores StringViews, nulls buffer,
and zero or more string buffers. String buffers store strings longer than 12
bytes. Strings in string buffers can appear in any order. String buffers may
contain more data than is used by the vector.

Let’s create a vector of strings. First, allocate a values buffer to hold 100
StringViews and initialize them to empty strings. Then, form a
FlatVector<StingView> around that values buffer.

.. code-block:: c++

    #include "velox/vector/FlatVector.h"

    BufferPtr values =
       AlignedBuffer::allocate<StringView>(100, pool.get(), StringView());
    auto vector = std::make_shared<FlatVector<StringView>>(
       pool.get(), VARCHAR(), nullptr, 100, values, std::vector<BufferPtr>{});

    LOG(INFO) << vector->toString();

    > [FLAT VARCHAR: 100 elements, no nulls]

Note that values buffers that hold strings must be initialized explicitly,
otherwise 4 bytes that are supposed to store the length of a string will
contain garbage values and accessing StringView with such sizes will lead to
failures or crashes.

Let’s see what happens if we forget to initialize the StringView buffer, i.e.
do not pass StringView() 3-rd argument to AlignedBuffer::allocate.

.. code-block:: c++

    // Danger!!! Creating a buffer with uninitialized content.
    BufferPtr values = AlignedBuffer::allocate<StringView>(100, pool.get());
    auto vector = std::make_shared<FlatVector<StringView>>(
       pool.get(), VARCHAR(), nullptr, 100, values, std::vector<BufferPtr>{});

    LOG(INFO) << vector->toString();
    LOG(INFO) << "Size of the string in row 5: " << vector->valueAt(5).size();
    LOG(INFO) << vector->toString(5);

    > [FLAT VARCHAR: 100 elements, no nulls]
    > Size of the string in row 5: 2711724449
    Process finished with exit code 139 (interrupted by signal 11: SIGSEGV)

The process crashed trying to print a string of size 2711724449. No surprise.

If we run this code under ASAN, we get an ERROR: AddressSanitizer: unknown-crash.

.. code-block:: c++

    > [FLAT VARCHAR: 100 elements, no nulls]
    > Size of the string in row 5: 2711724449

    =================================================================
    ==1753639==ERROR: AddressSanitizer: unknown-crash on address 0xa1a1a1a1a1a1a1a1 at pc 0x00000125c2c2 bp 0x7fff780d6050 sp 0x7fff780d5810
    READ of size 2711724449 at 0xa1a1a1a1a1a1a1a1 thread T0
    SCARINESS: 16 (multi-byte-read)
    #0 0x125c2c1 in __asan_memcpy
    #1 0x5fc750 in std::char_traits<char>::copy(char*, char const*, unsigned long) libgcc/include/c++/trunk/bits/char_traits.h:409
    #2 0x5fc661 in std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char>>::_S_copy(char*, char const*, unsigned long) libgcc/include/c++/trunk/bits/basic_string.h:359
    #3 0x5fc177 in std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char>>::_S_copy_chars(char*, char const*, char const*) libgcc/include/c++/trunk/bits/basic_string.h:406
    ...
    #9 0x9764c7 in facebook::velox::SimpleVector<facebook::velox::StringView>::valueToString[abi:cxx11](facebook::velox::StringView) const vector/SimpleVector.h:195
    #10 0x93f05c in facebook::velox::SimpleVector<facebook::velox::StringView>::toString[abi:cxx11](int) const vector/SimpleVector.h:205

We can also use BaseVector::create static method to make a vector. This method
is preferred because it requires less code and it makes sure that the values
buffer has the right size and is initialized properly.

.. code-block:: c++

    #include "velox/vector/FlatVector.h"

    auto vector = BaseVector::create(VARCHAR(), 100, pool.get());
    LOG(INFO) << vector->toString();

    > [FLAT VARCHAR: 100 elements, no nulls]

BaseVector::create() returns std::shared_ptr<BaseVector> (a.k.a. VectorPtr). If
we want a shared pointer to FlatVector<StringView> we can specify a template
parameter.

.. code-block:: c++

    auto vector = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 100, pool.get());

Now vector is an std::shared_ptr<FlatVector<StringView>>
(a.k.a. FlatVectorPtr<StringView>) and we can easily access methods defined
only for FlatVectors. For example, we can check the number of string buffers.

.. code-block:: c++

    LOG(INFO) << vector->stringBuffers().size();

    > 0

We don’t have any non-empty strings in the vector, so there are no string
buffers. Let’s write some strings.

.. code-block:: c++

    for (auto i = 0; i < 100; ++i) {
     if (i % 3 == 0) {
       vector->set(i, "RED");
     } else if (i % 3 == 1) {
       vector->set(i, "GREEN");
     } else if (i % 3 == 2) {
       vector->set(i, "BLUE");
     }
    }

    LOG(INFO) << "Number of string buffers: " << vector->stringBuffers().size();
    LOG(INFO) << vector->toString(0, 10);

    > Number of string buffers: 0
    > 0: RED
      1: GREEN
      2: BLUE
      3: RED
      4: GREEN

The vector now has non-empty strings, but still no string buffers. That’s
because we wrote only short strings (<= 12 bytes each). These strings are fully
stored within the StringViews inside the values buffer and do not require
separate string buffers.

Let’s now write some long strings.

.. code-block:: c++

    for (auto i = 0; i < 100; ++i) {
     if (i % 3 == 0) {
       vector->set(i, "In my hometown where I used to stay");
     } else if (i % 3 == 1) {
       vector->set(i, "The name of the place is Augusta, GA");
     } else if (i % 3 == 2) {
       vector->set(i, "Down there we have a good time");
     }
    }

    LOG(INFO) << "Number of string buffers: " << vector->stringBuffers().size();
    LOG(INFO) << vector->toString(0, 10);

    > Number of string buffers: 1
    > 0: In my hometown where I used to stay
      1: The name of the place is Augusta, GA
      2: Down there we have a good time
      3: In my hometown where I used to stay
      4: The name of the place is Augusta, GA

    const auto& stringBuffer = vector->stringBuffers()[0];
    LOG(INFO) << stringBuffer->size() << ", " << stringBuffer->capacity();

    > 3368, 49056

    LOG(INFO) << std::string_view(stringBuffer->as<char>(), 100);

    > In my hometown where I used to stayThe name of the place is Augusta, GADown there we have a good tim

Now, we see that there is one string buffer of size 3368 bytes and total
capacity of 49056 bytes. We can also see that this buffer contains the strings
we wrote one after another.

If we change some strings, the new strings will be written to the end of the
buffer and if we exceed the buffer's capacity, a new buffer will be allocated.

.. code-block:: c++

    for (auto n = 0; n < 25; ++n) {
        for (auto i = 0; i < 100; i += 2) {
            vector->set(i, "We all get together in time, for rhythm then we do");
        }

        LOG(INFO) << n << ": Number of string buffers: "
                  << vector->stringBuffers().size();

        for (const auto& buffer : vector->stringBuffers()) {
          LOG(INFO) << buffer->size() << ", " << buffer->capacity();
        }
    }

    > 0: Number of string buffers: 1
    > 5868, 49056
    ...
    > 17: Number of string buffers: 1
    > 48368, 49056

    > 18: Number of string buffers: 2
    > 49018, 49056
    > 1850, 49056

FlatVector<StringView>::set(index, value) method copies the string to the end of
a string buffer and updates StringView in the values buffer to point to that
copy. The old string is not removed and still occupies space in the buffer. We
need to be careful not to end up with many buffers full of unreferenced
strings. Also, FlatVector<StringView>::set doesn't check if the "new" string
already exists. There is no de-duplication logic.

We can also provide our own string buffers and make StringViews that refer to
these. Let’s create a buffer to hold the 3 strings we used above, then fill in
vector with StringViews pointing to these strings.

.. code-block:: c++

    const char* s1 = "In my hometown where I used to stay";
    const char* s2 = "The name of the place is Augusta, GA";
    const char* s3 = "Down there we have a good time";

    BufferPtr buffer = AlignedBuffer::allocate<char>(200, pool.get());
    auto* rawBuffer = buffer->asMutable<char>();

    int32_t offset1 = 0;
    memcpy(rawBuffer + offset1, s1, strlen(s1));

    int32_t offset2 = offset1 + strlen(s1);
    memcpy(rawBuffer + offset2, s2, strlen(s2));

    int32_t offset3 = offset2 + strlen(s2);
    memcpy(rawBuffer + offset3, s3, strlen(s3));

We have a string buffer. Let's create a vector using that buffer. We will use
the FlatVector<StringView>::setStringBuffers() method.

.. code-block:: c++

    auto vector = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 100, pool.get());
    vector->setStringBuffers({buffer});

    LOG(INFO) << "Number of string buffers: " << vector->stringBuffers().size();

    const auto& stringBuffer = vector->stringBuffers()[0];
    LOG(INFO) << stringBuffer->size() << ", " << stringBuffer->capacity();

    > Number of string buffers: 1
    > 200, 288

    LOG(INFO) << vector->toString(0, 10);

    > 0:
      1:
      2:
      3:

We have a vector with a string buffer, but all strings are still empty. Let’s
now populate the strings in the vector using StringViews that refer to the
string buffer. We are going to use the FlatVector<StringView>::setNoCopy method.

.. code-block:: c++

    for (auto i = 0; i < 100; ++i) {
     if (i % 3 == 0) {
       vector->setNoCopy(i, StringView(rawBuffer + offset1, strlen(s1)));
     } else if (i % 3 == 1) {
       vector->setNoCopy(i, StringView(rawBuffer + offset2, strlen(s2)));
     } else if (i % 3 == 2) {
       vector->setNoCopy(i, StringView(rawBuffer + offset3, strlen(s3)));
     }
    }

    LOG(INFO) << "Number of string buffers: " << vector->stringBuffers().size();

    const auto& stringBuffer = vector->stringBuffers()[0];
    LOG(INFO) << stringBuffer->size() << ", " << stringBuffer->capacity();

    > Number of string buffers: 1
    > 200, 288

    LOG(INFO) << vector->toString(0, 10);

    > 0: In my hometown where I used to stay
      1: The name of the place is Augusta, GA
      2: Down there we have a good time
      3: In my hometown where I used to stay
      4: The name of the place is Augusta, GA

The setNoCopy method takes a StringView and copies it into the values buffer. It
assumes that StringView is either inline (represents a short string) or points
to a valid location in one of the string buffers.

This design enables many zero-copy optimizations. For example, we can create a
new vector of sub-strings without copying any string. Let’s say we have a
vector of strings and we want to create a vector of substrings starting at
first character and going for up to 20 bytes.

.. code-block:: c++

    auto substr = BaseVector::create<FlatVector<StringView>>(VARCHAR(), 100, pool.get());
    substr->acquireSharedStringBuffers(vector.get());

We created a new string vector and used acquireSharedStringBuffers method to
copy smart pointers for the string buffers from the original vector into this
new vector.

We can now populate the strings using the setNoCopy method.

.. code-block:: c++

    for (auto i = 0; i < 100; ++i) {
     substr->setNoCopy(i, StringView(vector->valueAt(i).data(), 20));
    }

    LOG(INFO) << substr->toString();
    LOG(INFO) << substr->toString(0, 10);

    > 0: In my hometown where I used to stay
      1: The name of the place is Augusta, GA
      2: Down there we have a good time
      3: In my hometown where I used to stay
      4: The name of the place is Augusta, GA

And verify that the new vector has only one string buffer and it is the same
buffer as in the original vector.

.. code-block:: c++

    LOG(INFO) << substr->stringBuffers().size();
    LOG(INFO) << (void*)substr->stringBuffers()[0]->as<char>();
    LOG(INFO) << (void*)vector->stringBuffers()[0]->as<char>();

    > 1
    > 0x7fa1c011b400
    > 0x7fa1c011b400

In this chapter we have learned how to create string vectors. In the next
chapter we’ll look into how to create vectors of arrays.
