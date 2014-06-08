/*
 *  search.js
 *  Search out our dictionary on Twitter
 */

var redis = require('redis').createClient();
var twitter = require('ntwitter');

var elasticsearch = require('elasticsearch');
var es = new elasticsearch.Client({
  host: 'localhost:9200'/*,
  log: 'trace'*/
});

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
    setTimeout(function() {
      new SearchAPI(acc);
    }, requestLapse * i);
  });
})();

function storeEntities(entities) {
  var bulk = [];

  // Store entities in redis
  Object.keys(entities).forEach(function(type) {
    entities[type].forEach(function(item) {
      bulk = bulk.concat([{
        index: {
          _index: 'entities',
          _type: type
        }
      },
      item]);
    });
  });

  if (bulk.length) {
    es.bulk({ body: bulk }, function(err, res) {
      // console.log(res);
    });
  }
}

function store(data, q) {
  var bulk = [];

  // Store users and tweets in ES
  data.forEach(function(item) {
    if (item.lang != 'es') console.log(item.lang);

    if (!item.user) {
      console.log(item);
      return;
    }

    // The search term we used to find this tweet
    if (!q) {
      q = item.user.screen_name;
    }

    // Index users
    bulk = bulk.concat([{
      index: {
        _index: 'twitter',
        _type: 'user',
        _id: item.user.id_str + ''
      }
    },
    item.user]);
    delete item.user;

    item.term = q;
    // Index tweets
    bulk = bulk.concat([{
      index: {
        _index: 'twitter',
        _type: 'tweet',
        _id: item.id_str + ''
      }
    },
    item]);

    var message = {
      id: item.id_str,
      created_at: (new Date(item.created_at)).toISOString(),
      text: item.text,
      term: item.term,
      urls: item.entities.urls.map(function(u) {
        return u.url;
      })
    };
//    console.log(message);
    bulk = bulk.concat([{
      index: {
        _index: 'stream',
        _type: 'message',
        _id: item.id_str + ''
      }
    },
    message]);

    storeEntities(item.entities);
  });

  if (bulk.length) {
    es.bulk({ body: bulk }, function(err, res) {
      // console.log(res);
    });
  }
}

function SearchAPI(acc) {
  this.twitter = new twitter(acc);
  this.stream();
}

SearchAPI.prototype.next = function() {
  var delay = windowLapse / rateLimit;
  setTimeout(function() {
    this.stream();
  }.bind(this), delay);
};

SearchAPI.prototype.stream = function() {
  var api = this;

  this.twitter.getHomeTimeline({
    count: 200
  }, function(err, data) {
    api.next();

    if (err || !data) {
      console.log(err);
      return;
    }

    var n = data.length;

    console.log("\n\nStream");
    console.log("============================================================================================");
    console.log(n);

    store(data);
  });
};