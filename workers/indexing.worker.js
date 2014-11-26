var elasticsearch = require('elasticsearch');
var es = new elasticsearch.Client({
  host: 'localhost:9200'/*,
  log: 'trace'*/
});
var resolve = require('../lib/resolve');

module.exports = { 'indexing': store };

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
      resolve('indexed');
    });
  }
}