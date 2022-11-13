## Presto Native Worker Protocol Code Generation

###Required Dependencies:

Install [PyYAML](https://pyyaml.org/) and [Chevron](https://github.com/noahmorrison/chevron)

```
 $ pip3 install pyyaml chevron
```

Presto repo must be present at `$HOME/presto`. The `PRESTO_HOME` environment variable can
be set to override this location.

###Presto Protocol Code Generation:

Make necessary changes to `presto_protocol.yml` or to the files in the `special` directory
and run
```
 $ make
```

The C++ protocol classes are generated as `presto_prococol.h` and `presto_prococol.cpp`.

Check-in these generated files. **DO NOT EDIT THESE FILES MANUALLY.**

###Implementation Notes:


Presto Worker Protocol continues to evolve and is implemented in the Presto Java code base.
For the Native Worker, instead of manually implementing the corresponding
protocol structures, we try to automatically translate the Java classes.

The C++ protocol code generation involves 2 steps
1. A python script `java-to-struct-json.py` uses the `presto_protocol.yml` config
file, files inside the `special` directory to  produce a description of the
   protocol structures in the `presto_protocol.json` file.
The `presto_protocol.yml` config file is further described  below.

2. `Chevron` now uses this `presto_protocol.json` as input to the
[mustache](http://mustache.github.io/mustache.5.html) files
`presto_protocol-json-hpp.mustache`, `presto_protocol-json-cpp.mustache` and
generates the C++ protocol files `presto_protocol.h`, `presto_protocol.cpp`.
   
###presto_protocol.yml
The config file contains the following yaml format entries.

 * `JavaClasses` list the Java files from the Presto repo. Jackson Annotations in the Java
    classes are used in the translation.
 * `AbstractClasses` contains a list of Java classes. Each abstract class
  contains some properties and a list of their subclasses. Each subclass contains
  a name and a key.
 * `EnumMap` is used to specify the enum names in a Java class.
 * `ExtraFields` allow adding additional fields to a class.

Additional Java files can be added to the `special` directory. You might want to add these
if certain Java classes are not available in source form in the Presto repo or need
to be edited.

You can also override the generated C++ classes. Custom serde logic can be specified in
 `special/*.inc` files. The file name must match the class name.
To properly order the resulting C++ declarations, the `.inc` files can be annotated
with `//dependency` comments. These comments may appear in two places. Annotation
following a field declaration creates a dependency of this class on the data
type of that field. Annotations of the form `//dependency other_class`
create a dependency of this class on `other_class`.