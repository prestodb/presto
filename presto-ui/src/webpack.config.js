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
    const routerOutputDir = 'target/webapp-router';
    const baseConfig = {
        entry: {
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
                    {from: "static", to: path.join(__dirname, "..", outputDir)},
                ]
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
            path: path.join(__dirname, '..', outputDir),
            filename: '[name].js',
            chunkFilename: '[name].chunk.js',
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
    };

    const devServer = {
        static: {
            directory: path.join(__dirname, '..', outputDir),
        },
        proxy: [
            {
                context: ['/v1'],
                target: `http://${apiHost}:${apiPort}`,
                // secure: false, // when using http
                // changeOrigin: true, // Modify the Origin header to match the target
            },
        ],
    }

    const mainConfig = {
        ...baseConfig,
        entry: {
            'index': './index.jsx',
            'query': './query.jsx',
            'plan': './plan.jsx',
            'embedded_plan': './embedded_plan.jsx',
            'stage': './stage.jsx',
            'worker': './worker.jsx',
            'timeline': './timeline.jsx',
            'res_groups': './res_groups.jsx',
            'sql_client': './sql_client.jsx',
            'dev/query_viewer': './query_viewer.jsx',
            ...baseConfig.entry,
        },
        optimization: {
            ...baseConfig.optimization,
            splitChunks: {
                chunks: 'async',
                maxSize: 244000,
                minRemainingSize: 0,
                minChunks: 1,
                cacheGroups: {
                    defaultVendors: {
                        test: /[\\/]node_modules[\\/]/,
                        priority: -10,
                        reuseExistingChunk: true,
                        filename: '[name].vendor.js',
                    },
                    default: {
                        minChunks: 2,
                        priority: -20,
                        reuseExistingChunk: true,
                    },
                },
            },
        },
        devServer,
    };

    const routerConfig = {
        ...baseConfig,
        entry: {
            'index': './router/index.jsx',
        },
        optimization: {
            ...baseConfig.optimization,
            splitChunks: {
                ...mainConfig.optimization.splitChunks,
            }
        },
        output: {
            path: path.join(__dirname, '..', routerOutputDir),
            filename: '[name].js',
            chunkFilename: '[name].chunk.js',
        },
        plugins: [
            new CopyPlugin({
                patterns: [
                    {from: "static/index.html", to: path.join(__dirname, "..", routerOutputDir)},
                ]
            }),
        ],
    };

    const spaConfig = {
        ...baseConfig,
        plugins: [
            ...baseConfig.plugins,
            new HtmlWebpackPlugin({
                inject: 'body',
                filename: path.join('dev', 'query_viewer_spa.html'),
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
                assetPreservePattern: [
                    /.*.js$/,
                ]
            }),
        ],
        entry: {
            'query_viewer': {import: './query_viewer.jsx', filename: path.join('dev', '[name].js')},
            ...baseConfig.entry,
        },
        optimization: {
            ...baseConfig.optimization,
            splitChunks: false,
        }
    };

    if (env.config === 'all') {
        return [mainConfig, spaConfig, routerConfig];
    }
    if (env.config === 'spa') {
        return {
            ...spaConfig,
            devServer,
        }
    }
    return [mainConfig, routerConfig];
};
