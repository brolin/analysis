var redis = require('redis').createClient();
var _ = require('underscore');

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

(function generarCasosPorUbicacion() {
    var count = 0;
    var ubicaciones = {};

    function save(data, cb) {
        data.forEach(function(d) {
            var _ubicacion = d._source._ubicacion;
            _ubicacion.forEach(function(u) {
                ubicaciones[u] = ubicaciones[u] || [];
                ubicaciones[u].push(d._id);
            });
        });
        cb();
    }

    function scroll(scrollId) {
        es.scroll(scrollId, '30m', function(err, data) {
            var hits = JSON.parse(data).hits.hits;
            count += hits.length;

            if(hits.length) {
                save(hits, function() {
                    scroll(scrollId);
                });                
            }
            else {
                console.log("TOTAL HITS: "+count);
                redis.hmset('nocheyniebla:ubicacion:casos2', ubicaciones, function() {
                    redis.quit();
                    process.exit();
                });
            }
        });
    }

    es.search('nocheyniebla', 'reporte', {}, { search_type: 'scan', scroll: '30m', size: 300 }, function(err, data) {
        var scrollId = JSON.parse(data)._scroll_id;
        scroll(scrollId);
    });
});

(function contarCasosFaltantes() {
    var casos1 = {};
    var casos2 = {};
    
    (function getCasos1() {
        redis.hgetall('nocheyniebla:ubicacion:casos:ok', function(err, data) {
            var ubicaciones = Object.keys(data);
            
            ubicaciones.forEach(function(u) {
                var _casos = data[u].split(',');
                _casos.forEach(function(caso) {
                    casos1[caso] = u;
                });
            });
            
            getCasos2();
        });
    })();
    
    function getCasos2() {
        redis.hgetall('nocheyniebla:ubicacion:casos2', function(err, data) {
            var ubicaciones = Object.keys(data);
            
            ubicaciones.forEach(function(u) {
                var _casos = data[u].split(',');
                _casos.forEach(function(caso) {
                    casos2[caso] = u;
                });
            });
            
            getDifference();
        });
    }
    
    function getDifference() {
        var diff = _.difference(Object.keys(casos2), Object.keys(casos1));
        var ubicaciones = {};
        diff.forEach(function(caso) {
            var u = casos2[caso];
            ubicaciones[u] = ubicaciones[u] || [];
            ubicaciones[u].push(caso); 
        });
        redis.hmset('nocheyniebla:ubicacion:casos', ubicaciones, end);
    }
    
    function end() {
        redis.quit();
    }
});

(function agregarCasosFaltantes() {
    
});