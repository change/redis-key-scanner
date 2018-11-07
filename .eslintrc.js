module.exports = {
  extends: ['change-base', 'plugin:security/recommended'],

  plugins: ['security'],

  rules: {
    'object-curly-newline': ['error', {
      ObjectPattern: { multiline: true },
    }],
  },
};
