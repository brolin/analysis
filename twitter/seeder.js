var agent = require('superagent');
var cheerio = require('cheerio');
var _ = require('underscore');
var fs = require('fs');
var engine   = require('../../../../crawler_engine'),
    utils    = engine.utils;

var redis = require('redis').createClient();

var departamentos = [], municipios = [], barriosBogota = [], dictionary = [];

(function getDepartamentos() {
    agent.get('http://es.wikipedia.org/wiki/Departamentos_de_Colombia', function(res) {
        var $ = cheerio.load(res.text);
        var tds = $('#mw-content-text table').last().find('tr')
            .map(function() {
                var $td = $(this).find('td').first();
                return $td.text().trim().toLowerCase();
            });
        tds.shift(); tds.pop();
        departamentos = tds;

        redis.sadd('dictionary', departamentos, function() {
            redis.quit();
        });
    });
});

(function getMunicipios() {
    var deptos = [];

    function eq(depto) {
        console.log();
        console.log(depto);
        console.log('=================================================');
        switch(depto) {
            case 'santander':
            case 'tolima':
                return 0;
            case 'caldas':
                return 2;
            case 'cesar':
                return 3;
            default:
                return 1;
        }
    }

    function _getMunicipios(url) {
        
        if(!url) {
            // console.log('TOTAL: '+municipios.length);
            redis.quit();
            return;
        }

        url = decodeURI('http://es.wikipedia.org'+url);
        var depto = url.split(':').pop().replace(/Municipios_de(l?)_/, '').replace(/_/g, ' ').toLowerCase();
        depto = depto.replace(/\(colombia\)/, '').trim();

        var _eq = eq(depto);
        agent.get(url, function(res) {
            var $ = cheerio.load(res.text);

            var _municipios = $('#mw-content-text table.sortable.wikitable').last().find('tr')
                .map(function() {
                    // console.log($(this).find('td').eq(_eq).find('a').text());
                    return $(this).find('td').eq(_eq).find('a').text().trim().toLowerCase();
                });

            _municipios = _.compact(_municipios);

            redis.sadd('dictionary', _municipios);
            _municipios.forEach(function(m) {
                redis.sadd('dictionary', m+' '+depto);
            });

            municipios = municipios.concat(_municipios);

            next();
        });
    }

    function next() {
        _getMunicipios(deptos.shift());
    }

    agent.get('http://es.wikipedia.org/wiki/Municipios_de_Colombia', function(res) {
        var $ = cheerio.load(res.text);
        deptos = $('#mw-content-text table').last().find('tr')
            .map(function() {
                var $a = $(this).find('td').eq(1).find('a');
                if($a.length)
                    return $a.attr('href').trim();
            });
        deptos = _.compact(deptos);
        deptos.pop(); deptos.splice(3, 1);
        
        next();
    });
});

(function getBarriosBogota() {
    var localidades, body;

    function _getBarrios(localidad) {

        if(!localidad) {
            // Write to redis
            redis.sadd('dictionary', barriosBogota);
            redis.quit();
            return;
        }

        var ctx = {
            body: body,
            //proxy: '',
            headers: utils.headersByAgent({
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1468.0 Safari/537.36'
            }),

            //jar: request.jar(),
            repeat: {
                times: 2,
                delay: 1000 // 1000
            },
            wait: 1000, // 1000
            timeout: 1000,
            level: 10,
            trace: []
        };

        var opts = utils.createFormRequest(ctx);
        opts.url = 'http://www.bogotamiciudad.com/MapasBogota/MapasDeBogota.aspx';
        opts.form['ctl00$ContentLeft$ddLocalidad'] = opts.form2['ctl00$ContentLeft$ddLocalidad'] = localidad;

        opts.statusOk = [ 302, 200 ];
        opts.followAllRedirects = true;
        utils.performChainRequest(ctx, opts, function() {
            var $ = cheerio.load(ctx.body);
            var barrios = $('#ctl00_ContentLeft_ddBarrio option')
                .map(function() {
                    return $(this).text().trim();
                });
            barrios = _.compact(barrios);
            
            barriosBogota = barriosBogota.concat(barrios);
            barriosBogota = barriosBogota.concat(barrios.map(function(b) {
                return b+' bogota';
            }));
            barriosBogota = barriosBogota.concat(barrios.map(function(b) {
                return b+' '+localidad;
            }));

            next();
        });
    }

    function next() {
        _getBarrios(localidades.shift());
    }

    agent.get('http://www.bogotamiciudad.com/MapasBogota/MapasDeBogota.aspx', function(res) {
        body = res.text;
        var $ = cheerio.load(body);
        localidades = $('form').find('select option').map(function() {
            // Agregar el nombre de la localidad
            var l = $(this).text().trim();
            barriosBogota.push(l);
            barriosBogota.push(l+' bogota');
            return $(this).attr('value')
        });
        // Skip the first id: '0'
        localidades.shift();

        next();
    });
});

(function getBarriosMedellinCali() {
    var ciudad, comuna, dictionary = [];

    fs.readFile('barrios.txt', 'utf8', function(err, data) {
        data = data.split('\n');
        data = _.compact(data);

        data.forEach(function(l) {
            var isCiudad = l.match(/^>\s(.+)/);
            if(isCiudad) {
                ciudad = isCiudad[1].trim().toLowerCase();
                return;
            }

            var isComuna = l.match(/>>\s(.+)/);
            if(isComuna) {
                comuna = isComuna[1].trim().toLowerCase();
                dictionary.push(comuna+' '+ciudad);
                return;
            }
            
            var barrio = l.toLowerCase();
            if(!isCiudad && !isComuna) {
                dictionary.push(barrio+' '+comuna);
                dictionary.push(barrio+' '+ciudad);
                dictionary.push(barrio+' '+comuna+' '+ciudad);
            }
        });

        redis.sadd('dictionary', dictionary, function() {
            redis.quit();
        });
    });
});

(function getMunicipiosLocation() {
    var deptos = [];
    var municipios = [];

    agent.get('http://es.wikipedia.org/wiki/Municipios_de_Colombia', function(res) {
        var $ = cheerio.load(res.text);
        deptos = $('#mw-content-text table').last().find('tr')
            .map(function() {
                var $a = $(this).find('td').eq(1).find('a');
                if($a.length)
                    return $a.attr('href').trim();
            });
        deptos = _.compact(deptos);
        deptos.pop(); deptos.splice(3, 1);
        
        nextDepto();
    });

    function eq(depto) {
        console.log();
        console.log(depto);
        console.log('=================================================');
        switch(depto) {
            case 'santander':
            case 'tolima':
                return 0;
            case 'caldas':
                return 2;
            case 'cesar':
                return 3;
            default:
                return 1;
        }
    }

    function _getMunicipios(url) {
        
        if(!url) {
            nextLocation();
            return;
        }

        url = decodeURI('http://es.wikipedia.org'+url);
        var depto = url.split(':').pop().replace(/Municipios_de(l?)_/, '').replace(/_/g, ' ').toLowerCase();
        depto = depto.replace(/\(colombia\)/, '').trim();

        var _eq = eq(depto);
        agent.get(url, function(res) {
            var $ = cheerio.load(res.text);

            var _municipios = $('#mw-content-text table.sortable.wikitable').last().find('tr')
                .map(function() {
                    var muni = $(this).find('td').eq(_eq).find('a').text().trim().toLowerCase();
                    return muni+'+'+depto+'+colombia';
                });

            _municipios = _.compact(_municipios);
            municipios = municipios.concat(_municipios);
            nextDepto();
        });
    }

    function nextDepto() {
        _getMunicipios(deptos.shift());
    }

    function nextLocation() {
        var q = municipios.shift();
        console.log(q);
        if(!q) {
            redis.quit();
            return;
        }

        var url = "http://nominatim.openstreetmap.org/search?format=json&q="+q;
        agent.get(url, function(res) {
            var muni = q.split('+');
            muni.pop();

            var places = JSON.parse(res.text);
            console.log(places);
            var first = places.filter(function(p) {
                return p.type == 'town';
            }).shift() || places.shift();
            try {
                var location = [first.lat, first.lon];
                console.log(muni);
                console.log(location);
                redis.hset('municipios:location2', muni, location);
                nextLocation();                
            } catch(e) {
                console.log(places);
                nextLocation();
            }
        });
    }

})();