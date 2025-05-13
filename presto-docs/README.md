# Building the documentation

Presto documentation is authored in `rst` format and published using [Sphinx](https://www.sphinx-doc.org). The rst files are located in `presto-docs/src/main/sphinx` and sub-folders.

## Prerequisites

Building the presto-docs module requires Python3 with virtual environment. You can check if you have it installed by running:
```shell
python3 -m venv --help
```

To install venv:
```shell
python3 -m pip install --user virtualenv
```

Optionally, the PDF version of `presto-docs` requires LaTeX tooling.

For MacOS,
```shell
brew install --cask mactex
```

For Ubuntu,
```shell
sudo apt-get update
sudo apt-get install -y texlive-fonts-recommended texlive-latex-recommended texlive-latex-extra latexmk tex-gyre texlive-xetex fonts-freefont-otf xindy
```


## Building manually
The default build uses Apache Maven to download dependencies and generate the HTML. You can run it as follows:
```shell
cd presto-docs
mvn install
```
Or, to build the documentation more quickly:
```shell
cd presto-docs
./build
```
To build PDF version of the documentation
```shell
cd presto-docs
./build --with-pdf
```

## Viewing the documentation
When the build is complete, you'll find the output HTML files in the `target/html/` folder.

To view the docs, you can open the file `target/html/index.html` in a web browser on macOS with
```shell
open target/html/index.html
```
Or, you can start a web server e.g., using Python:
```shell
cd target/html/
python3 -m http.server
```
and open [http://localhost:8000](http://localhost:8000) in a web browser.
