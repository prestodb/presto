/* global __dirname */
const process = require('process');
const TerserPlugin = require("terser-webpack-plugin");
const mode = process.argv.includes('-p') ? 'production' : 'development';

module.exports = {
    entry: {
        'index': __dirname +'/index.jsx',
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
                            ['@babel/preset-env', {targets: "defaults"}],
                            ['@babel/preset-react', {runtime: "automatic"}],
                            ['@babel/preset-flow']
                        ],
                    },
                }
            },
        ]
    },
    resolve: {
        extensions: ['.*', '.js', '.jsx']
    },
    output: {
        path: __dirname + '/../dist',
        filename: '[name].js'
    },
    optimization: {
        minimize: mode === 'production',
        minimizer: [
            new TerserPlugin({
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
