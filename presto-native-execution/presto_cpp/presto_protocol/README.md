## Hacked JSON serde for Presto worker

The serialization / de-serialization code generator works by extracting JSON
Java annotations from Presto source files and generating similar C++ classes.
Two files are generated:
  * presto_prococol.h
  * presto_prococol.cpp

Top level supported classes:

  * TaskInfo
  * TaskStatus
  * TaskUpdateRequest

Dependencies to Install

   PyYAML

   pip3 insall pyyaml

   chevron

   pip3 install chevron

   You will need to note the path to the installed executable and add this to your PATH.
   Something like `/Users/adutta/Library/Python/3.8/bin`.

Implementation Notes:

* Recursive types, abstract types and optionals are implemented as shared_ptr
values.  They should be tested for nullptr before use.
* Subclasses of abstract types have an additional "_type" field of type
std::string that indicates the subclass of that instance.

## java-to-struct-json.py

A python script that produces a description of the protocol structures in
presto_protocol.json.  The config file in presto_protocol.yml controls the
source generation.

 * Abstract classes, their subclasses and the subclass type key are listed in
 AbstractClasses.

 * Java files from Presto are listed in JavaClasses.  The script defaults the
presto repo checkout to $HOME/presto.  The PRESTO_HOME environment variable can
be set to override this location.

 * ExtraProps lists a few additional java class properties that failed to be
 extracted via Jackson annotations.

A few additional java files are found in ./special/*.java  These are either not
available in source form in the Presto repo or needed to be lightly edited.

Classes with custom serde logic are in ./special/*.inc files in this directory.
Each class must be in a file that is the same as the class name.  To properly
order the resulting C++ declarations .inc files can be annotated with "//
dependency" comments.  These comments may appear in two places.  Annotations
following a field declaration create a dependency of this class on the data
type of that field.  Annotations of the form : ```// dependency other_class```
create a dependency of this class on the other class.

## chevron

[chevron](https://github.com/noahmorrison/chevron) is run twice once to produce
the header, presto_prococol.h, and again to produce the presto_prococol.cpp file.
Two mustache templates control source generation
  * presto_protocol-json-hpp.mustache
  * presto_protocol-json-cpp.mustache

The required declaration and templates are at the top of each of these files.
