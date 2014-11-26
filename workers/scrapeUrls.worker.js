var emit = require('../lib/emit');
var $a = require('async');
var request = require('superagent');
var redis = require('redis').createClient();
var noop = function() {};
var Browser = require('zombie');

module.exports = { 'scrapeUrls': scrape };

function scrape(urls, callback) {

  if(typeof urls === 'string') { urls = [urls]; }

  $a.mapSeries(urls, function(cb) {
    try {
      var engine = new ParseEngine(url);
      engine.parse(function(err, result) {
        console.log(result);
        cb(err, result);
      });
    } catch(e) {
      console.error(e);
      cb({ error: e });
    };
  }, callback)
}

scrape(['http://t.co/WGzjCR09qo', 'http://t.co/DtLQmlKfA9', 'http://t.co/h9kNpzww35', 'http://t.co/MrynH88bDo', 'http://t.co/wWHBXN2Twi', 'http://t.co/1bX30N2X7Y'], function(data) {
 console.log(data);
});