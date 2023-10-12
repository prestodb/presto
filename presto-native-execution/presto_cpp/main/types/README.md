presto_cpp uses antlr4 to parse types.  
Details on how to create a lexer and parser using antlr4 are available here:  
https://github.com/antlr/antlr4/blob/master/doc/cpp-target.md  

**TypeSignature.g4** file specifies the grammar. Any new rules must be added to this file.   
presto_cpp requires the antlr4 **visitor** and does not need the antlr4 **listener**.

To generate antlr files:

+ Download https://repo1.maven.org/maven2/org/antlr/antlr4/4.9.3/antlr4-4.9.3-complete.jar
+ Run

    java -jar <path/to>/antlr4-4.9.3-complete.jar -Dlanguage=Cpp -package facebook::presto::type -visitor -no-listener -o antlr TypeSignature.g4

The following files are generated inside the antlr directory:
+ TypeSignature.interp  
+ TypeSignature.tokens
+ TypeSignatureBaseVisitor.cpp
+ TypeSignatureBaseVisitor.h
+ TypeSignatureLexer.cpp
+ TypeSignatureLexer.h
+ TypeSignatureLexer.interp
+ TypeSignatureLexer.tokens
+ TypeSignatureParser.cpp
+ TypeSignatureParser.h
+ TypeSignatureVisitor.cpp
+ TypeSignatureVisitor.h

The generated .cpp and .h files need to be modified as described below:
   1) Invoke 'make header-fix' to include the copyright header in the generated files.
   2) Encapsulate code in the namespace facebook::presto::type.
