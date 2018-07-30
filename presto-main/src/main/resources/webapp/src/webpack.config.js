module.exports = {
    entry: {
        'index': __dirname +'/index.js',
        'query': __dirname +'/query.js',
        'plan': __dirname +'/plan.js',
        'stage': __dirname +'/stage.js',
    },
    mode: "development",
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
