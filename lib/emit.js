var resque = require('coffee-resque').connect({
  host: 'cos',
  port: 6379
});

module.exports = function(msg) {
  resque.enqueue(msg, msg, [data]);
};

