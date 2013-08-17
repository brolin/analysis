var redis = require('redis').createClient();

function deleteTable() {
    redis.smembers('stream:following', function(err, data) {
        data.forEach(function(i) {
            redis.del('stream:following:'+i, function(err, del) {
                console.log(del);
            });
        });
    });
}

(function updateTags() {
    redis.hgetall('terms:tags', function(err, data) {
        var terms = {};
        Object.keys(data).map(function(k) {
            data[k].split(',').forEach(function(tag) {
                terms[tag] = terms[tag]||[];
                terms[tag].push(k);
            });
        });
        console.log(terms);
        redis.hmset('tags:terms', terms, function() {
            redis.quit();
        });
    });
})();