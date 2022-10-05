# Presto Native Execution Documentation

We use Sphinx to generate documentation and GitHub pages to publish it.
- Sphinx: https://pythonhosted.org/an_example_pypi_project/sphinx.html
- GitHub pages: https://pages.github.com/

## Building

To install Sphinx: `easy_install -U sphinx`

`sphinx-quickstart` command was used to generate the initial Makefile and config.

To build the documentation, e.g. generate HTML files from .rst files:

`cd presto_cpp/docs && make html`

Navigate to
`presto_cpp/docs/_build/html/index.html` in your browser to view the documentation.

## Publishing

GitHub pages is configured to display the contents of the top-level docs directory
found in the gh-pages branch. The documentation is available at
https://expert-adventure-b62ae804.pages.github.io/.

To publish updated documentation copy the contents of the _build/html
directory to the top-level docs folder and push to gh-pages branch.

```
# Make sure 'master' is updated to the top of the tree.
# Make a new branch.
git checkout -b update-docs master

# Generate the documentation.
cd presto_cpp/docs
make html

# Copy documentation files to the top-level docs folder.
cp -R _build/html/* ../../docs

# Commit the changes.
git add ../../
git commit -m "Update documentation"

# Update gh-pages branch in the upstream.
# This will get the website updated.
git push -f upstream update-docs:gh-pages
```
