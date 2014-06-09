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
    return f.split('.').pop().pop() === 'worker';
  });
  return cb(null, files);
}

function startWorkers(workers, cb) {
  $a.each(workers, function(name) {
    var worker = WorkerFactory.create({ name: name });
    worker.run();
  });
}