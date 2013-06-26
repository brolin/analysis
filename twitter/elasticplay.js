// var redis = require('redis').createClient();

var esclient = (function() {
    var fork = true;
    if(fork) {
        return require('/Users/santiago/Projects/node-elasticsearch-client');
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
var remote = (function() {
    var opts = {
        host: 'cos',
        port: 9200
    };

    return new (esclient)(opts);
})();

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

// Add Mapping with custom Analyzer
(function putMapping() {
    
    var mapping = {
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
                    "type": "string",
                    "analyzer": "es_tweetAnalyzer",
                    "store": "yes"
                }
            }
        }
    };

    es.putMapping('twitter', 'message', mapping,appings)
        .on('data', function(data) {
            console.log(data);
        })
        .exec();
});

// Create Index
(function createIndex() {
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
});

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

// Use scrolling to export from tweet index to test index
(function exports() {
    var count = 0;

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
                if(count > 250000) return;
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

    es.search('twitter', 'tweet', {}, { search_type: 'scan', scroll: '15m', size: 100 }, function(err, data) {
        var scrollId = JSON.parse(data)._scroll_id;
        scroll(scrollId);
    });
})();

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
})();

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