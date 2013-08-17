var agent = require('superagent');
var cheerio = require('cheerio');
var _ = require('underscore');
var fs = require('fs');
var utils   = require('../../crawler_engine/lib/utils');

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
            var muni = _.compact(q.split('+'));
            var places = JSON.parse(res.text);

            var first = places.filter(function(p) {
                return p.type == 'town';
            }).shift() || places.shift();
            try {
                var location = [first.lat, first.lon];
                console.log(muni);
                console.log(location);
                redis.hset('municipios:location', muni, location);
                nextLocation();                
            } catch(e) {
                console.log(places);
                nextLocation();
            }
        });
    }

});

(function getNocheYNiebla() {
    var departamentos,  clasificaciones, body, currentDepto, cookie, csrf;

    function store(records) {
        var bulk = [];

        // Store reports in ES
        records.forEach(function(reporte) {
            bulk = bulk.concat([
                { index: { _index: 'nocheyniebla', _type: 'reporte', _id: +'' } },
                reporte
            ]);
        });

        if(bulk.length) {
            es.bulk(bulk, function(err, res) {
                // console.log(res);
            });            
        }
    };

    function getByDepartamentoAndClasificaciones(clasificacion) {
        var data = {};
        data['evita_csrf'] = 984;
        data['_qf_default:consultaWeb'] = 'id_departamento';
        data['id_departamento'] = '5';
        data['clasificacion[]'] = 'A:1:13';
        data['critetiqueta'] = '0';
        data['orden'] = 'fecha';
        data['mostrar'] = 'tabla';
        data['caso_memo'] = '1';
        data['caso_fecha'] = '1';
        data['m_ubicacion'] = '1';
        data['m_victimas'] = '1';
        data['m_presponsables'] = '1';
        data['m_tipificacion'] = '1';
        data['_qf_consultaWeb_consulta'] = 'Consulta';
        
        console.log('Consultando ...');
        agent
            .post('https://www.nocheyniebla.org/consulta_web.php')
            .send(data)
            .set('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
            .set('Content-Type', 'application/x-www-form-urlencoded')
            .set('Cookie', cookie)
            .end(function(error, res){
                var $ = cheerio.load(res.text);
                var records = $('table tr').map(function MapHTMLTable() {
                    var record = {};
                    var $row = $(this);

                    $row.find('td').each(function(i) {
                        var field = ['descripcion', 'fecha', 'ubicacion', 'victimas', 'responsable', 'tipificacion'][i];
                        record[field] = $(this).text();
                        var normalizer = {
                            responsable: function() {
                                return record[field].split(',').map(function(t) { return t.trim() });
                            },
                            ubicacion: function() {
                                var _m = record[field].split('/').map(function(t) { 
                                    return t.toLowerCase().trim();
                                });
                                _m.splice(2);
                                _m.reverse();
                                return _m.join(',');
                            },
                            tipificacion: function() {
                                return record[field].match(/([A-D]:\d+:\d+)/g);
                            }
                        };
                        if(normalizer[field]) {
                            record['_'+field] = normalizer[field]();
                        }
                    });
                    return record;
                });
                // The first row is the table's header
                records.shift();
                store(records/*, next*/);
            
            });
    }

    function next(err) {
        if(err) {
            console.log(err);
        }
        
        var clasificacion = clasificaciones.shift();
        
        if(!clasificacion) {
            clasificaciones.push(0);
            currentDepto = departamentos.shift();
            next();
            return;
        }

        getByDepartamentoAndClasificaciones(clasificacion);
        clasificaciones.push(clasificacion);
    }
    
    
    agent.get('https://www.nocheyniebla.org/consulta_web.php', function(res) {
        var $ = cheerio.load(res.text);
        body = '<html xmlns="http://www.w3.org/1999/xhtml"><head></head><body>'+$('form').toString()+'</body></html>';

        csrf = $('form').find('[name=evita_csrf]').attr('value');
        cookie = (res.headers['set-cookie']);
        
        clasificaciones = $('form').find('[name=clasificacion\\[\\]] option').map(function() {
            // Agregar el nombre de la localidad
            return $(this).text().trim();
        });
        // Mark the head of clasificaciones. Look next()
        clasificaciones.unshift(0);
        
        departamentos = $('form').find('[name=id_departamento] option').map(function() {
            // Skip the first id: '0'
             return $(this).text().trim();
        });
        next();
    });
})();