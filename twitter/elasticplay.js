// var redis = require('redis').createClient();

var esclient = (function() {
    var fork = true;
    if(fork) {
        return require('/Projects/node-elasticsearch-client');
    }
    return require('elasticsearchclient');
})();

// Initialize ES
var es = (function() {
    var opts = {
        host: 'localhost',
        port: 9200
    };

    return new (esclient)(opts);
})();
// Remote ES
/*var remote = (function() {
    var opts = {
        host: 'cos',
        port: 9200
    };

    return new (esclient)(opts);
})();*/

// Match Search
(function match() {
	var qryObj = {
      "query" : {
            "match" : { "text" : "farc" }
        }
    };

    es.search('twitter', 'tweet', qryObj)
        .on('data', function(data) {
            // console.log(typeof JSON.parse(data));
            var hits = JSON.parse(data).hits.hits;
            hits.forEach(function(tweet) {
                console.log(tweet._source.text);
            });
        })
        .exec();
});

// Term Search
(function term() {
    var qryObj = {
      "query" : {
            "term" : { "text" : "farc" }
        }
    };

    es.search('twitter', 'tweet', qryObj)
        .on('data', function(data) {
            // console.log(typeof JSON.parse(data));
            var hits = JSON.parse(data).hits.hits;
            hits.forEach(function(tweet) {
                console.log(tweet._source.text);
            });
        })
        .exec();
});

// Create Analysis Index
(function createTwitterIndex() {
    es.deleteIndex('twitter', createIndex);

    var settings = {
        "settings": {
            "index": {
                "index.mapper.dynamic": false
            }
        }
    }

    function createIndex() {
        es.createIndex('twitter', settings)
            .on('data', function(data) {
                console.log(data);
            })
            .exec();
    }
})();

// Create Analysis Index
(function createAnalysisIndex() {
    var settings = {
        "settings": {
            "index": {
                "analysis": {
                    "char_filter" : {
                        "remove_punctuation" : {
                            "type" : "mapping",
                            "mappings" : [ ".=>-", ",=>-", ";=>-" ]
                        }
                    },
                    "filter": {
                        "es_stop_filter": {
                            "type": "stop",
                            "stopwords": [ "_spanish_", "d", "q", "tal" ]
                        },
                        "es_stem_filter": {
                            "type": "stemmer",
                            "name": "minimal_portuguese"
                        },
                        "shingles_filter": {
                            "type": "shingle",
                            "output_unigrams": false
                        }
                    },
                    "analyzer": {
                        "es_tweetAnalyzer": {
                            "type": "custom",
                            "tokenizer": "icu_tokenizer",
                            "char_filter" : [ "remove_punctuation" ],
                            "filter": [
                                "icu_folding", 
                                "icu_normalizer", 
                                "es_stop_filter"
                            ]
                        },
                        "shinglesAnalyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "char_filter" : [ "remove_punctuation" ],
                            "filter": [
                                "icu_folding", 
                                "icu_normalizer",
                                "shingles_filter"
                            ]
                        }
                    }
                }
            }
        },
        "mappings": {
            "message": {
                "_source": {
                    "enabled": true
                },
                "_all": {
                    "enabled": false
                },
                "index.query.default_field": "text",
                "properties": {
                    "id": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "user_id": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "created_at": {
                        "type": "date"
                    },
                    "text": {
                        "type": "multi_field",
                        "fields": {
                            "text": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                            "terms": {
                                "type": "string",
                                "index": "analyzed",
                                "analyzer": "es_tweetAnalyzer",
                                "store": "yes"
                            },
                            "shingles": {
                                "type": "string",
                                "index": "analyzed",
                                "analyzer": "shinglesAnalyzer",
                                "store": "yes"
                            }
                        }
                    }
                }
            }
        }
    };

    es.deleteIndex('analysis', createIndex);

    function createIndex() {
        es.createIndex('analysis', settings)
            .on('data', function(data) {
                console.log(data);
            })
            .exec();
    }
})();

// Update Settings
(function updateSettings() {

    es.closeIndex('twitter')
        .on('data', function(data) {
            console.log(data);
            updateSettings();
        });

    function updateSettings(done) {
        es.updateSettings('twitter', settings)
            .on('data', function(data) {
                console.log(JSON.parse(data));
            })
            .exec();        
    }
});


(function exports() {
    function save(data, cb) {
        var bulk = [];
        data.forEach(function(d) {
            var item = {
                id: d._source.id_str,
                created_at: (new Date(d._source.created_at)).toISOString(),
                text: d._source.text
            };
            bulk = bulk.concat([
                { index: { _index: 'analysis', _type: 'message', _id: item.id } },
                item
            ]);
        });

        if(bulk.length) {
            es.bulk(bulk, function(err, res) {
                cb();
            });            
        }    
    }

    function scroll(scrollId) {
        es.scroll(scrollId, '15m', function(err, data) {
            var hits = JSON.parse(data).hits.hits;
            count += hits.length;

            if(hits.length) {
                save(hits, function() {
                    scroll(scrollId);
                });                
            }
            else {
                console.log("TOTAL HITS: "+count);
            }
        });
    }

    es.search('twitter', 'tweet', {}, { search_type: 'scan', scroll: '15m', size: 300 }, function(err, data) {
        var scrollId = JSON.parse(data)._scroll_id;
        scroll(scrollId);
    });
});

// Export original tweets to remote
(function exportRawToRemote() {
    var count = 0;

    setInterval(function() {
        console.log("COUNT: "+count)
    }, 60000)

    function save(data, cb) {
        var bulk = [];
        data.forEach(function(d) {
            if(!d._source.id_str) return;

            var item = d._source;
            bulk = bulk.concat([
                { index: { _index: 'twitter', _type: 'tweet', _id: item.id_str } },
                item
            ]);
        });

        if(bulk.length) {
            remote.bulk(bulk, function(err, res) {
                cb();
            });   
        }    
    }

    function scroll(scrollId) {
        es.scroll(scrollId, '360m', function(err, data) {
            var hits = JSON.parse(data).hits.hits;
            count += hits.length;

            if(hits.length) {
                save(hits, function() {
                    scroll(scrollId);
                });                
            }
            else {
                console.log("TOTAL HITS: "+count);
            }
        });
    }

    es.search('twitter', 'tweet', {}, { search_type: 'scan', scroll: '360m', size: 300 }, function(err, data) {
        var scrollId = JSON.parse(data)._scroll_id;
        scroll(scrollId);
    });
});

// Filter Facet
(function filterFacet() {

    (function getTerms() {
        redis.smembers('dictionary', function(err, data) {
            doSearch(data);
        });
    })();

    function doSearch(terms) {
        var facet = {
            "query": {
                "match_all": {}
            },
            "filter": {
                "terms": { "text": [ "antioquia", "medellin", "cali", "barranquilla" ] }
            },
            "facets" : {
                "dictionary" : {
                    "terms" : {
                        "field" : "text",
                        "all_terms": false
                    },
                    "facet_filter": {
                        "terms": { "text": [ "antioquia", "medellin", "cali", "barranquilla" ] }
                    }
                }
            }
        };

        var facet2 = {
            "query": {
                "filtered": {
                    "query": { "match_all": {} },
                    "filter": {
                        "terms": { "text": [ "bogota", "antioquia", "medellin", "cali", "barranquilla", "bucaramanga", "cucuta", "pasto", "manizales", "caldas", "santander" ] }
                    }
                }
            },
            "facets": {
                "dictionary": {
                    "terms": {
                        "field": "text",
                        "size": "10"
                    },
                }                
            }
        };

        es.search('test', 'message', facet, function(err, data) {
            console.log(data);
        });
    }
});

// Terms filter
(function termsFilter() {
    
    (function getTerms() {
        redis.smembers('dictionary', function(err, data) {
            doSearch(data);
        });
    })();

    function doSearch(terms) {

        var query = {
            "filtered": {
                "query": {
                    "match_all": {}
                },
                "filter": {
                    "terms": terms
                }
            }
        };

    }
});

// This will go through result to detect what shingles
// must be excluded
(function excludeShinglesByCode() {
    var back_regex = "(\\s)(\\d+|0|1|2|3|a|c|e|i|o|q|en|yo|tu|ti|tus|ellos|nos|su|sus|por|desde|hacia|hasta|en|al|de|del|el|le|la|lo|las|los|les|con|no|y|t|que|me|para|da|san|mi|mis|un|una|te|es|esa|ese|eso|esos|esta|este|estos|estas|ya|se|como|with|of|for|gt|lt|http|to|be|the|in|on|co|at|you|it|si|ya|va|ser|hay|hacer|ve|sea|muy|ir|ver|hoy|todo|puede|ha|era|soy|vez|otro|otros|mas|sino|tras|pra|uno|cuando|sin|tal|vez|estar|pero|ah|pues)$";
    var front_regex = "^(\\d+|um|em|pra|a|e|o|yo|ya|al|y|i|de|desde|os|estos|si|se|en|es|and|of|for|my|the|to|at|this|in|is|it|on|with|han|via|con|tu|tus|te|ti|su|sus|un|una|unas|por|me|mi|mis|no|nos|que|del|you|que|este|esta|le|les|lo|mas|para|el|la|las|los|ya|estoy|eu|gt|lt|ha|he|muy|buen|buena|buenos|buenas|sino|san|santa|tras|otro|otros|uno|puede|cada|cuando|vez|ni|estar|pero|ah|pues)(\\s)";
    var back_matcher = new RegExp(back_regex, "i");
    var front_matcher = new RegExp(front_regex, "i");
    
    var shinglesQuery = function(cb) {
        redis.smembers('exclude_shingles', function(err, data) {
            cb({
                "size": "1000",
                "query": {
                    "match_all": {}
                },
                "facets": {
                    "blah": {
                        "terms": {
                            "field": "text.shingles",
                            "size": "1000",
                            "exclude": data
                        }
                    }
                }
            });            
        });
    };

    search();
    
    function search() {
        shinglesQuery(doSearch);
        
        function doSearch(query) {
            es.search('analysis', 'message', query, function(err, data) {
                
            });
        }
    }
});