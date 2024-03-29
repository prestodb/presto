# Generate JavaScript Lexer, Listener, and Parser from the ANTLR Grammar

The JavaScript code in this directory is generated by [antlr4-tools](https://github.com/antlr/antlr4-tools). Python3 is needed
to run the tool.

### Install antlr4-tools

```sh
pip install antlr4-tools
```

### Generate JavaScript Code
The ANTLR grammar file of Presto is located at
[presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4](https://github.com/prestodb/presto/blob/master/presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4).
Use the `antlr4` to generate the lexer, parser, and listener JavaScript code from the grammar file:
```sh
antlr4 -Dlanguage=JavaScript -o . -Xexact-output-dir <location of SqlBase.g4>
```

To use the JavaScript code, you also need the [ANTLR runtime](https://www.npmjs.com/package/antlr4). For detailed information about how to use
the generated parser and listener, you can check the documentation [here](https://github.com/antlr/antlr4/blob/master/doc/javascript-target.md).
In the SQL client, both parser and listener are used. Make sure you generate both of them.