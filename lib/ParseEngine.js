#!/usr/bin/env node

var util = require('util'),
    async = require('async'),
    Browser = require('zombie'),
    Article = require('../model/article.js'),
    config = require('../config'),
    BaseParser = require('../lib/base_parser');
require('uri');

function log(m) {
  if(config.debug) {
    console.log(m||'');
  }
}

function parseUrl = function(uri) {
  if (!uri || typeof uri !== 'string') {
      return null;
  }

  try {
    var u = new URI(uri);
    var result = {
      host: u.heirpart().authority().host() + '.js',
      domain: u.heirpart().authority().host().replace(/^[^.]*\./,'') + '.js',
      feed: uri.replace(/^http:\/\//,'').replace(/\//g,'_') + '.js'
    };
  } catch(e) {
    return null;
  }
  return result;
};

var Iconv = require('iconv').Iconv;

Buffer.prototype._$_toString = Buffer.prototype.toString;
Buffer.prototype.toString = function (charset) {
  if (typeof charset == 'undefined' || charset == 'utf8' || charset == 'utf16le' || charset == 'ascii' || charset == 'ucs2' || charset == 'binary' || charset == 'base64' || charset == 'hex') {
    return this._$_toString.apply(this, arguments);
  }
  var iconv = new Iconv(charset, 'UTF-8');
  var buffer = iconv.convert(this);
  var args = arguments;
  args[0] = 'utf8';
  return buffer.toString.apply(buffer, args);
}

module.exports = ParseEngine;

/**
 * New parser instance
 *
 * @throws Error
 * @constructor
 */
function ParseEngine() {

  /**
   * Check 'item' is valid URL and starts with http:// or https://
   */
  if (!item || item.length < 9 || item.substr(0, 4) != 'http') {
    throw new Error('INVALID_URL');
  }

  this.item = item;

  // Load parser
  var parserModule = this.loadParser();
  if(parserModule instanceof Error) {
    throw parserModule;
    return;
  }
  this.pageParser = parserModule;
}

/**
 * Initialize all resources here
 */
ParseEngine.prototype.init = function() {
};

/**
 * Check if parser contains all needed methods
 * @param {Object} parser
 * @returns {boolean}
 */
ParseEngine.prototype.isValidParser = function(parser) {
  if (typeof parser !== 'object' && typeof parser !== 'function') {
    return false;
  }

  if ('title' in parser &&
      'html' in parser &&
      'images' in parser &&
      'author' in parser &&
      'publishDate' in parser &&
      'getBrowserOptions' in parser) {
    return true;
  }

  return false;
};

/**
 * Load parser
 *
 */
ParseEngine.prototype.loadParser = function(callback) {
  try {
    var modulePath = __dirname + '/../parsers/';
    modulePath += parseUrl(this.item).host;
    var Parser = require(modulePath);
    return new Parser();
  } catch(e) {
    return new Error('Could not load a parser. Make sure a parser for this item exists and it is written properly.');
  }
};

ParseEngine.prototype.__text = function(selector) {
  var text = this.browser.text(selector);
  if (typeof text !== 'string') {
    return null;
  }
  return text;
};

ParseEngine.prototype.text = function(selector) {
  var nodes = this.browser.queryAll(selector);
  var result = [];
  nodes.forEach(function(el) {
    var text = el.textContent.trim();
    if(!text) { return; }

    if(text.match(/(:|,|;|”|\?|\!)$/)) {
    } else if(!text.match(/\.$/)) {
      text += '.';
    }
    result.push(text);
  });
  return result.join(' ');
};

ParseEngine.prototype.html = function(selector) {
  return this.browser.html(selector);
};

ParseEngine.prototype.element = function(selector) {
  var el = this.browser.query(selector);
  if(!el) { return null; }
  var element = {
    name: el.tagName,
    attributes: {},
    content: el.textContent,
    html_content: el.innerHTML
  };

  for (var i=0, attrs=el.attributes, l=attrs.length; i<l; i++) {
    var attr = attrs.item(i);
    element.attributes[attr.nodeName] = attr.nodeValue;
  }

  return element;
};

ParseEngine.prototype.findOne = ParseEngine.prototype.element;

ParseEngine.prototype.find = function(selector) {
  var nodes = this.browser.queryAll(selector);
  if(!nodes) { return null; }

  var results = nodes.map(function(el) {
    var element = {
      name: el.tagName,
      attributes: {},
      content: el.textContent,
      html_content: el.innerHTML,
      parent: {
        name: el.parentNode.tagName,
        content: el.parentNode.textContent
      }
    };

    for (var i=0, attrs=el.attributes, l=attrs.length; i<l; i++) {
      var attr = attrs.item(i);
      element.attributes[attr.nodeName] = attr.nodeValue;
    }

    return element;
  });

  return results;
};

ParseEngine.prototype.remove = function(selector, context) {
  var nodes = this.browser.queryAll(selector, context);
  nodes.forEach(function(el) {
    el.parentNode.removeChild(el);
  });
};

/* Name: bupRemove (bottom-up remover)
 * INFO: more useful for lasts elements that are not important (external stuff),
 * because of the way 'selector' is set ( e.g. ...:nth-last-child(1) );
 * In conclusion, it acts like a cleaning action.
 *
 * @parameters:
 * selector: selector for one element (in our tests, last 'parent' that has an 'em' [see usage])
 * parent_tag: element that is containing the targeted element
 *
 * @usage example:
 * parser.bupRemove("div.primary > p:nth-last-child(1) > em", "p");
 */
ParseEngine.prototype.bupRemove = function (selector, parent_tag) {
  var nodes = this.browser.querySelectorAll(selector);

  while(nodes.length > 0) {
      var node = nodes[0]; // focus on the first one found only (see "usage": because one p could have 2 em's, removing parent (p) would remove both)

      // get parentNode's tag name:
      var t_name = node.parentNode.tagName;

      if (parent_tag.toLowerCase() === t_name.toLowerCase()) {
        // remove the parentNode if it corresponds to our case:
        node.parentNode.parentNode
          .removeChild(node.parentNode);
      } else {
        // remove only the selected element:
        node.parentNode
          .removeChild(node);
      }


    // checking for others:
    nodes = this.browser.querySelectorAll(selector);
  }
}

/*
* @checks for presence of 'bad_text' (regex) inside 'text'
* @return TRUE (if matched) or FALSE (not matched)
* @parameters: container, text to check for
*/
ParseEngine.prototype.badContent = function(text, bad_text) {
  var i;
  for(i=0; i<bad_text.length;i++) {
    if (text.match(bad_text[i])) {
      return true;
    }
  }

  return false;
}

/*
* @info: removes any element that is empty (:empty css selector not being enough)
* @reason: because in some cases it's really complicated to target a node because of empty nodes.
* @usage: parser.removeEmpty("body p");
* */
ParseEngine.prototype.removeEmpty = function (selector) {
  var nodes = this.browser.querySelectorAll(selector);
  nodes.forEach(function(n) {
    if(n.textContent.trim().length === 0) {
      n.parentNode.removeChild(n);
    }
  });
}


ParseEngine.prototype.images = function(selectors) {
  var images = this.browser.queryAll(selectors);
  return images.map(function(el, i) {
    return el.getAttribute('src');
  });
};

ParseEngine.prototype.parseDate = function(dateStr) {
  var date = new Date(Date.parse(dateStr));
  var month = date.getMonth() + 1;
  if (month <= 9) {
    month = '0' + month;
  }

  var day = date.getDate();
  if (day <= 9) {
    day = '0' + day;
  }

  return date.getFullYear() + '/' + month + '/' + day;
};

ParseEngine.prototype.parse = function(callback) {
  var parser = this;
  var browserOptions = {
    debug: false,
    features: 'no-scripts no-css',
    // waitFor: 5000,
    maxWait: 15000,
    silent: true,
    loadCSS: false
  };

  /**
   * Check page parser (that we loaded)
   */
  if (!this.pageParser || !this.isValidParser(this.pageParser)) {
    return callback(new Error(nolet.enum.errors.PARSER.NOT_FOUND), null);
  }
  /**
    * Merge default options with domain specific
    *
    * @type {*}
    */
  var options = nolet.helper.merge(browserOptions, this.pageParser.getBrowserOptions(this));

  try {
    /**
     * Create new Browser object
     *
     * @type {Browser}
     */
    var browser = new Browser(options);
    this.browser = browser;

    /**
     * Register error handler
     */
    browser.on('error', function(e) {
      console.log("Browser error %s", e);
    });

    this.pageParser.init(browser, this, function(err, r) {
      if (err) {
        return callback(err, null);
      }
    });

    /**
     * Call mock objects
     */
    log('Adding mock objects to Browser');
    this.pageParser.addMocks(browser, this);

    /**
     * Open & parse page
     */
    browser.visit(this.item).then(function () {
      if (!browser.success) {
        log(browser.errors);
        return callback(new Error(nolet.enum.errors.PARSER.BROWSER_ERROR), null);
      }

      /**
       * Extract all fields in parallel
       */
      async.series({
        /*html: function(cb) {
          log('Fetching HTML');
          cb(null, parser.pageParser.html(parser));
        },*/
        title: function(cb) {
          log('Fetching title');
          cb(null, parser.pageParser.title(parser));
        },
        content: function(cb) {
          log('Fetching content');
          cb(null, parser.pageParser.content(parser));
        },
        author: function(cb) {
          log('Fetching author');
          var author = parser.pageParser.author(parser);
          author = author || nolet.helper.parseUrl(parser.item).host;
          cb(null, author);
        },
        publishDate: function(cb) {
          log('Fetching publishDate');
          cb(null, parser.pageParser.publishDate(parser));
        },
        images: function(cb) {
          log('Fetching images');
          cb(null, parser.pageParser.images(parser));
        }
      }, callback);
    })
  } catch(e) {
    callback(e, null);
  }
}

try {
  var engine = new ParseEngine()
  engine.parse(function(err, result) {
    if(err) {
      if(process.send) {
        process.send({ error: err.toString() });
      } else {
        console.log(err);
      }
      process.exit(1);
    }

    if(process.send) {
      process.send(result);
    } else {
      // We know console.log is a wrapper of process.stdout
      console.log(JSON.stringify(result, null, 2));
    }
  })
} catch(e) {
  console.log(e);
  process.exit(1);
};
