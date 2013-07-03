Modules
====================

Terms Analysis
====================

Features
---------------------

### monograms
Monograms are built from a terms facet:

Analysis is done by es_tweetAnalyzer:
`https://github.com/santiago/analysis/blob/master/twitter/elasticplay.js#L116`


### bigrams
Bigrams are built from a terms facet:

Analysis is done by 
`https://github.com/santiago/analysis/blob/master/twitter/elasticplay.js#L126`


API
---------------------

### GET /terms

### GET /terms/exclude

### POST /terms/exclude

### DELETE /terms/exclude

### GET /shingles

### GET /shingles/exclude

### POST /shingles/exclude

### DELETE /shingles/exclude

Elasticsearch
---------------------

### Indices:

* twitter: tweet, user
* analysis: message


License: CC. 
Santiago Gaviria