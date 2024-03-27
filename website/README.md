# Website

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern static website generator.

### Installation

```
$ yarn
```

### Local Development

```
$ yarn start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```
$ yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

### Deployment

Velox's website is automatically deployed using
[Netlify](https://www.netlify.com/). Whenever a pull request changing one of
the files under *velox/website* is submitted, a live preview link is generated
by Netlify. The link is posted in the pull request as a comment by the Netlify
bot. When the pull request is merged, the changes are automatically deployed to
the website by Netlify. 
