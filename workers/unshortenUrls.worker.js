var $a = require('async');
var request = require('superagent');
var redis = require('redis').createClient();

/*var WorkerBase = require('worker-base');
var worker = new WorkerBase();
worker.addJob('unshortenUrls', unshorten);
module.exports = worker;*/

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
    redis.hmset('colombia-analiza:urls', urlData, function() {});
    return callback();
  });
}

function qs(params) {
  return Object.keys(params).map(function(f) { return f+'='+params[f]; }).join('&');
}

function resolveUrl(url, urlExists, cb) {
  if(urlExists) { return cb(null, urlExists); }

  url = encodeURIComponent(url);

  var params = {
    'title': 1,
    'meta-keywords': 1,
    'meta-description': 1,
    'format': 'json'
  };
  url = 'http://api.longurl.org/v2/expand?url='+url+'&'+qs(params);
  request.get(url, function(err, res) {
    cb(null, res.body);
  });
}

function urlExists(url, cb) {
  redis.hget('colombia-analiza:urls', url, function(err, urlExists) {
    cb(null, url, urlExists && JSON.parse(urlExists));
  });
}

unshorten(['http://t.co/U12Ze5vzoT', 'http://bit.ly/1nnkq16', 'http://t.co/ogTE33fBG8', 'http://t.co/QRITc0J5zF'], function() {

});