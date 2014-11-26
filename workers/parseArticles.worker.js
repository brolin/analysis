var $a = require('async');
var request = require('superagent');
var redis = require('redis').createClient();
var noop = function() {};

module.exports = { 'parseArticles': parse };

function parse(urls, callback) {
  if(typeof urls === 'string') { urls = [urls]; }

}