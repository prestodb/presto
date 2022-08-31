// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Velox',
  tagline: 'The open source unified execution engine.',
  url: 'https://velox-lib.io',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'facebook', // Usually your GitHub org/user name.
  projectName: 'docusaurus', // Usually your repo name.

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Velox',
        logo: {
          alt: 'Velox Logo',
          src: 'img/logo.svg',
        },
        items: [
          {
            href: 'https://facebookincubator.github.io/velox/',
            label: 'Docs',
            position: 'left',
          },
          {
            href: '/docs',
            label: 'Roadmap',
            position: 'left',
          },
          {
            to: '/blog',
            label: 'Blog',
            position: 'left',
          },
          {
            href: 'https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md',
            label: 'Contributing',
            position: 'right',
          },
          {
            href: 'https://github.com/facebookincubator/velox',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Documentation',
            items: [
              {
                label: 'Velox in 10 Minutes',
                href: 'https://facebookincubator.github.io/velox/velox-in-10-min.html',
              },
              {
                label: 'Developer Guides',
                href: 'https://facebookincubator.github.io/velox/develop.html',
              },
              {
                label: 'Presto Functions',
                href: 'https://facebookincubator.github.io/velox/functions.html',
              },
              {
                label: 'Monthly Updates',
                href: 'https://facebookincubator.github.io/velox/monthly-updates.html',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Slack',
                href: 'https://velox-oss.slack.com',
              },
              {
                label: 'Contributing',
                href: 'https://github.com/facebookincubator/velox/blob/main/CONTRIBUTING.md',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/facebookincubator/velox',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Meta Platforms, Inc.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
