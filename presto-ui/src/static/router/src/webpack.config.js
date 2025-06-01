/* global __dirname */
const process = require('process');

const mode = process.argv.includes('-p') ? 'production' : 'development';

module.exports = {
    entry: {
        'index': __dirname +'/index.jsx',
    },
    mode,
    module: {
        rules: [
            {
                test: /\.(js|jsx)$/,
                exclude: /node_modules/,
                use: ['babel-loader']
            }
        ]
    },
    resolve: {
        extensions: ['*', '.js', '.jsx']
    },
    output: {
        path: __dirname + '/../dist',
        filename: '[name].js'
    }
};
