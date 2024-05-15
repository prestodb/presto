/* global __dirname */
const HtmlWebpackPlugin = require('html-webpack-plugin');
const HtmlInlineScriptPlugin = require('html-inline-script-webpack-plugin');
const path = require('node:path');
const TerserPlugin = require("terser-webpack-plugin");

module.exports = (env) => {
    const mode = env.production ? 'production' : 'development';
    return {
        entry: {
            'query_viewer': { import: path.join(__dirname, 'query_viewer.jsx'), filename: path.join('dev', '[name].js')},
            'bootstrap_css': path.join(__dirname, '..', 'vendor', 'bootstrap', 'css', 'bootstrap.min.external-fonts.css'),
            'css_loader': path.join(__dirname, '..', 'vendor', 'css-loaders', 'loader.css'),
            'css_loader': path.join(__dirname, '..', 'assets', 'presto.css'),
        },
        externals: {
            // substitutes `require('vis-timeline/standalone')` to `global.vis`
            'vis-timeline/standalone': 'vis',
        },
        mode,
        module: {
            rules: [
                {
                    test: /\.(?:js|jsx)$/,
                    exclude: /node_modules/,
                    use: {
                        loader: 'babel-loader',
                        options: {
                            presets: [
                                ['@babel/preset-env', { targets: "defaults" }],
                                ['@babel/preset-react', {runtime: "automatic"}],
                                ['@babel/preset-flow']
                            ],
                        },
                    }
                },
                {
                    test: /\.css$/i,
                    use: ["style-loader", "css-loader"],
                },
            ]
        },
        resolve: {
            extensions: ['.*', '.js', '.jsx']
        },
        output: {
            path: __dirname,
        },
        optimization: {
            minimize: mode === 'production',
            minimizer: [
              new TerserPlugin({
                // do not genreate *.LICENSE.txt files
                terserOptions: {
                  format: {
                    comments: false,
                  },
                },
                extractComments: false,
              }),
            , '...'],
          },
        plugins: [
            new HtmlWebpackPlugin({
              inject: 'body',
              filename: 'query_viewer.html',
              template: 'templates/query_viewer.html',
            }),
            new HtmlInlineScriptPlugin({
              htmlMatchPattern: [/query_viewer.html$/],
            }),
          ],
    };
};
