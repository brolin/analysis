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

(function agregarCasosFaltantes() {
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
        console.log(diff);
        var ubicaciones = {};
        diff.forEach(function(caso) {
            var u = casos2[caso];
            ubicaciones[u] = ubicaciones[u] || [];
            ubicaciones[u].push(caso); 
        });
//        redis.hmset('nocheyniebla:ubicacion:casos', ubicaciones, end);
    }
    
    function end() {
        redis.quit();
    }
});

(function actualizarUbicaciones() {

    var count = 0;

    setInterval(function() {
        console.log("COUNT: "+count)
    }, 60000)

    redis.hgetall('nocheyniebla:ubicacion:casos:ok', function(err, data) {
        var ubicaciones = Object.keys(data);
        var casos = {}

        ubicaciones.forEach(function(u) {
            var depto = u.split(',').pop();
            data[u].split(',').forEach(function(caso) {
                casos[caso] = casos[caso] || { ubicacion: [], depto: [] };
                casos[caso].ubicacion.push(u);
                casos[caso].depto.push(depto);
                
                casos[caso].ubicacion = _.uniq(casos[caso].ubicacion);
                casos[caso].depto = _.uniq(casos[caso].depto);
            });
        });

        var _casos = Object.keys(casos);
        doUpdate();
        function doUpdate() {
            if(!_casos.length) {
                console.log('done');
                redis.quit();
                return;
            }
            
            var bulk = [];
            var _updt = _casos.splice(0, 250);

            _updt.forEach(function(caso) {
                bulk.push({ "update" : { "_id" : caso, "_type" : "caso", "_index" : "nocheyniebla" } });
                bulk.push({ "doc" : { "_ubicacion" : casos[caso].ubicacion, "departamento": casos[caso].depto } });
            });

            es.bulk(bulk, function(err, res) {
                doUpdate();
            });
        }
    });
})();