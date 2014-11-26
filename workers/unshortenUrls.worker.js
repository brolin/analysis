var $a = require('async');
var request = require('superagent');
var redis = require('redis').createClient();
var noop = function() {};

module.exports = { 'unshortenUrls': unshorten };

function unshorten(urls, callback) {
  if(!urls || !urls.length) { return callback(); }
  $a.map(urls, function(url, cb) {
    $a.waterfall([
      urlExists.bind(null, url),
      resolveUrl
    ], cb);
  }, function(err, _urls) {
    var urlData = {};
    urls.forEach(function(url, i) {
      urlData[url] = JSON.stringify(_urls[i]);
    });
    return callback(urlData);
  });
}

function qs(params) {
  return Object.keys(params).map(function(f) { return f+'='+params[f]; }).join('&');
}

function resolveUrl(url, urlExists, cb) {
  if(urlExists) { return cb(null, urlExists); }
  var params = {
    'title': 1,
    'meta-keywords': 1,
    'meta-description': 1,
    'format': 'json'
  };

  var service = 'http://api.longurl.org/v2/expand?url=';
  service += encodeURIComponent(url)+'&'+qs(params);

  request.get(service, function(err, res) {
    // console.log('got '+url);
    // console.log(res.body);
    // console.log();
    redis.hset('colombia-analiza:urls', url, JSON.stringify(res.body), noop);
    cb(null, res.body);
  });
}

function urlExists(url, cb) {
  redis.hget('colombia-analiza:urls', url, function(err, urlExists) {
    cb(null, url, urlExists && JSON.parse(urlExists));
  });
}

// Usage
/*unshorten(['http://t.co/WGzjCR09qo', 'http://t.co/DtLQmlKfA9', 'http://t.co/h9kNpzww35', 'http://t.co/MrynH88bDo', 'http://t.co/wWHBXN2Twi', 'http://t.co/1bX30N2X7Y'], function(data) {
 console.log(data);
});*/