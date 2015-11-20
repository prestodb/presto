# Prestodb.io Website

This is the repo for the static content that is hosted on <prestodb.io>. Below are instructions for setting up a copy of the website locally and making changes.

The website is constructed using Jekyll and GitHub Flavored Markdown, the source files for which are stored in this branches `_site_src/` folder. Changes to the site are made to these source files, and then static files are built using Jekyll to the root folder of this branch. 

API reference docs are stored in `docs/` and are not built by this Jekyll process. 

## Making Changes

1. Install Jekyll http://jekyllrb.com/docs/installation/ on your machine. You'll also need `redcarpet` installed via gem.
2. Git clone this branch.
3. Make changes to the source files stored in `_site_src/`.
4. Once you're done making changes, open a command line, and run the following commands from the root folder of this branch:
    ```
      $ jekyll build --source _site_src/
          Generating...
                    done in 0.383 seconds.
      $ cp _site/* . -r
    ```
   Here we are building the source directory `_site_src/` into static content which will be stored in `_site/`. Then we're copying this static content out from the `_site/` directory, into the root where it will be served when pushed to this branch. 
5. Commit your changes.

### Testing Changes 

Jekyll includes a basic webserver that lets you run a copy of the website from the source files:

1. Open up `_config.yml` in the `_site_src/` folder and adjust `url` to `http://localhost:4000` *(no trailing slash)* and `baseurl` to an empty string - **Note you should not commit this change back to GitHub, it is only for local testing**.
2. cd to the `_site_src/` folder of the branch.
3. Run `jekyll serve`.
4. Open your browser and goto http://localhost:4000/

## Editing Basics

* Page content is mostly written using GitHub flavoured Markdown, but HTML works too - https://help.github.com/articles/github-flavored-markdown/
* Any .md files you create in the root of the `_site_src/` folder will become paths on the built website. So community.md becomes /community/ - this is all done automatically by Jekyll. 
* CSS content should me modified in the SCSS files (stored in `_site_src/scss`) which are built into regular CSS by Jekyll. 

### Adjusting the FAQ and Tools Pages

The contents of these page are stored in `_site_src/_data/` in YAML files (FAQ in `faq.yml` and Tools in `resources.yml`) to allow for automatic generation of in-page navigation. Take a look at the entries there to see how to structure additions. The text field allows Markdown.

### Adjusting Header Nav and Footer

`_site_src/_data` contains `nav.yml` which controls the header navigation options, and `footer_links.yml` which controls the footer navigation. 


