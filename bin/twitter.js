var emit = require('../lib/emit');
var twitter = require('ntwitter');

var accounts = [{
  consumer_key: 'VVAqxraNlgGbbf4Ip2w',
  consumer_secret: '9i0QQ5BhEPIF9SXa6g5cyT5d8vAI47cYsI7zbiodco',
  access_token_key: '1603952870-blAR7Ioqh7MChydAEcFKMfQ142ro56OUTn41yc0',
  access_token_secret: 'Um2aoo18GaP5Lz8yOtrat69mjkVFYxmPBeXDCjw9Zw'
}];

var terms = [];
var windowLapse = 15 * 60 * 1000;
var rateLimit = 15;
var requestLapse = (windowLapse / rateLimit) / accounts.length;

(function run() {
  accounts.forEach(function(acc, i) {
    setTimeout(getHomeTimeline, requestLapse * i);
  });
})();

function next() {
  var delay = windowLapse / rateLimit;
  setTimeout(function() {
    this.stream();
  }.bind(this), delay);
};

function getHomeTimeline() {
  var api = this;

  twitter.getHomeTimeline({
    count: 200
  }, function(err, data) {
    next();

    if (err || !data) {
      console.log(err);
      return;
    }

    var n = data.length;

    log("\n\nStream");
    log("============================================================================================");
    log(n);

    // Index tweet in ES
    emit('indexing', [data]);

    // Unshorten URLs
    var urls = ((data.entities||{}).urls||[]).map(function(url) { return url.url; });
    emit('unshortenUrls', [urls]);

    // Scrape URLs
    emit('scrapeUrls', [urls])
      .next('indexUrl')
      .next('indexOpenCalais');

    // Store to Graph DBs (Giraph, neo4j)
    // resque.enqueue('storeGraph', 'storeGraph', [data]);
  });
}