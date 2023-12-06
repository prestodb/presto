/* global __dirname */

const path = require('node:path');
const TerserPlugin = require("terser-webpack-plugin");

module.exports = (env) => {
    var mode = env.production ? 'production' : 'development';
    return {
        entry: {
            'index': path.join(__dirname, 'index.jsx'),
            'query': path.join(__dirname, 'query.jsx'),
            'plan': path.join(__dirname, 'plan.jsx'),
            'embedded_plan': path.join(__dirname, 'embedded_plan.jsx'),
            'stage': path.join(__dirname, 'stage.jsx'),
            'worker': path.join(__dirname, 'worker.jsx'),
            'timeline': path.join(__dirname, 'timeline.jsx'),
            'res_groups': path.join(__dirname, 'res_groups.jsx'),
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
                            ]
                        }
                    }
                },
            ]
        },
        resolve: {
            extensions: ['.*', '.js', '.jsx']
        },
        output: {
            path: path.join(__dirname, '..', 'dist'),
            filename: '[name].js'
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
    };
};
