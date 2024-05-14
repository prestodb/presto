/* global __dirname */

const path = require('node:path');
const TerserPlugin = require("terser-webpack-plugin");
const CopyPlugin = require("copy-webpack-plugin")
const HtmlWebpackPlugin = require('html-webpack-plugin');
const HtmlInlineScriptPlugin = require('html-inline-script-webpack-plugin');

module.exports = (env) => {
    const mode = env.production ? 'production' : 'development';
    const apiHost = env.apiHost || 'localhost';
    const apiPort = env.apiPort || '8080';
    const outputDir = 'target/webapp';
    return {
        entry: {
            'index': path.join(__dirname, 'index.jsx'),
            'query': path.join(__dirname, 'query.jsx'),
            'plan': path.join(__dirname, 'plan.jsx'),
            'query_viewer': {import: path.join(__dirname, 'query_viewer.jsx'), filename: path.join(outputDir, 'dev', '[name].js')},
            'embedded_plan': path.join(__dirname, 'embedded_plan.jsx'),
            'stage': path.join(__dirname, 'stage.jsx'),
            'worker': path.join(__dirname, 'worker.jsx'),
            'timeline': path.join(__dirname, 'timeline.jsx'),
            'res_groups': path.join(__dirname, 'res_groups.jsx'),
            'sql_client': path.join(__dirname, 'sql_client.jsx'),
            'bootstrap_css': path.join(__dirname, 'static', 'vendor', 'bootstrap', 'css', 'bootstrap.min.external-fonts.css'),
            'css_loader': path.join(__dirname, 'static', 'vendor', 'css-loaders', 'loader.css'),
            'css_presto': path.join(__dirname, 'static', 'assets', 'presto.css'),
        },
        externals: {
            // substitutes `require('vis-timeline/standalone')` to `global.vis`
            'vis-timeline/standalone': 'vis',
        },
                plugins: [
                    new CopyPlugin({
                        patterns: [
                            {from: "static", to: outputDir},
                        ]
                    }),
                    new HtmlWebpackPlugin({
                        inject: 'body',
                        filename: path.join(__dirname, '..', outputDir, 'dev', 'query_viewer_spa.html'),
                        template: 'templates/query_viewer.html',
                        chunks: [
                            'query_viewer',
                            'bootstrap_css',
                            'css_loader',
                            'css_presto',
                        ]
                    }),
                    new HtmlInlineScriptPlugin({
                        htmlMatchPattern: [/query_viewer_spa.html$/],
                    }),
                ],
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
                                ['@babel/preset-env', {targets: "defaults"}],
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
            path: path.join(__dirname, '..'),
            filename: path.join(outputDir, '[name].js'),
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
                '...'],
        },
        devServer: {
            static: {
                directory: path.join(__dirname, '..', outputDir),
            },
            proxy: {
                '/v1': `http://${apiHost}:${apiPort}`,
            },
        },
    };
};
