var resque = require('coffee-resque').connect({ host: 'cos' });
var $a = require('async');
var fs = require('fs');

// Run all jobs
$a.waterfall([
  fs.readdir.bind(fs, '.'),
  filterFiles,
  startWorkers
]);

function filterFiles(files, cb) {
  files = files.filter(function(f) {
    return f.split('.').splice(-2, 1) === 'worker';
  });
  return cb(null, files);
}

function startWorkers(files, cb) {
  $a.each(files, function(name) {
    var jobs = require('./'+name);
    var worker = resque.worker(name, jobs);
    console.log('Starting '+name);
    worker.start();
  });
}