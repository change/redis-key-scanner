module.exports = {
  extends: ['change-base', 'change-base/mocha'],

  rules: {
    'object-curly-newline': ['error', {
      ObjectPattern: { multiline: true },
    }],
  },
};
