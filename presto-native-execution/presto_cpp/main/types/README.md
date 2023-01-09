presto_cpp uses antlr4 to parse types.  
Details on how to create a lexer and parser using antlr4 are available here:  
https://github.com/antlr/antlr4/blob/master/doc/cpp-target.md  

**TypeSignature.g4** file specifies the grammar. Any new rules must be added to this file.   
presto_cpp requires the antlr4 **visitor** and does not need the antlr4 **listener**.  
antlr files are generated using the following command:  

    antlr4 -Dlanguage=Cpp -no-listener -visitor TypeSignature.g4 -o antlr

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
