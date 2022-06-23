# Building the documentation

Presto documentation is authored in `rst` format and published using [Sphinx](https://www.sphinx-doc.org). The rst files are located in `presto-docs/src/main/sphinx` and sub-folders.

## Prerequisites

Building the presto-docs module requires Python3 with virtual environment. You can check if you have it installed by running:
```
python3 -m venv --help
```

To install venv:
```
python3 -m pip install --user virtualenv
```

## Building manually
The default build uses Apache Maven to download dependencies and generate the HTML. You can run it as follows:
```
cd presto-docs
mvn install
```
Or, to build the documentation more quickly:
```
cd presto-docs
./build
```
## Viewing the documentation
When the build is complete, you'll find the output HTML files in the `target/html/` folder.

To view the docs, you can open the file `target/html/index.html` in a web browser on macOS with
```
open target/html/index.html
```
Or, you can start a web server e.g., using Python:
```
cd target/html/
python3 -m http.server
```
and open [http://localhost:8000](http://localhost:8000) in a web browser.
