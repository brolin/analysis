function validate(opts) {
  var invalid = null;

  opts = [
    { name: 'name', required: true, default: opts.name }
  ];
  opts.forEach(function(o) {
    // Validate whether it's mandatory value
    if(o.required && !default) {
      invalid = 'Field \''+name+'\' is missing';
    }
  });

  if(!invalid) { return opts; }
  throw Error(invalid);
}

function WorkerBase(opts) {
  this.opts = validate(opts);

  var name = opts.name;
  var jobs = {};

  var _service = null;

  this.setService = function(service) {
    _service = service;
  };

  this.run = function() {
    _service.worker(name, jobs);
    _service.start();
  };
}

WorkerBase.prototype.addJob = function(id, fn) {
  if(!id) { throw 'You must provide an id for this job'; }
  this.jobs[id] = fn;
};

var WorkerFactory = {
  create: function(opts) {
    opts = validate(opts);
    try {
      var worker = new WorkerBase({ name: opts.name });
      var workerService = require(__dirname+'/../'+workers+'/'+opts.name);
      return worker;
    } catch(e) {
      console.error('There is no worker named '+opts.name);
    }
  }
};

module.exports = WorkerFactory;