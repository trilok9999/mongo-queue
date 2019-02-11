'use strict';

// Handles creating a retry queue, and then setting up cron jobs to call it

var queueFn = require('./queue');
var cron = require('./cron');
var utils = require('./utils');

module.exports = function mongoQueue(opts) {
  // TODO: Add assertions to the options -- requireed mongoUrl, collectionName, onProcess
  var queue = queueFn({
    mongoUrl: opts.mongoUrl,
    collectionName: opts.collectionName,
    onProcess: opts.onProcess,
    batchSize: opts.batchSize || 20,
    maxRecordAge: opts.maxRecordAge,
    onFailure: opts.onFailure,
    retryLimit: opts.retryLimit,
    continueProcessingOnError: opts.continueProcessingOnError,
    backoffMs: opts.backoffMs,
    backoffCoefficient: opts.backoffCoefficient,
    onStatusesCheckProcess: opts.onStatusesCheckProcess
  });

  var processCronJob = cron.createJob({
    name: opts.collectionName + '-process',
    cron: opts.processCron,
    handlers: {
      processTick: queue.processNextBatch
    }
  });

  var cleanup;
  if (opts.cleanupCron) {
    var cleanupCronJob = cron.createJob({
      name: opts.collectionName + '-cleanup',
      cron: opts.cleanupCron,
      handlers: {
        processTick: queue.cleanup
      }
    });

    cleanup = cleanupCronJob.run;
  } else {
    cleanup = function() {
      return Promise.resolve().then(queue.cleanup);
    };
  }

  var statusesCheck;
  if (opts.statusesMonitorCron) {
    var statusesCheckCronJob = cron.createJob({
      name: opts.collectionName + '-statuses-check',
      cron: opts.statusesMonitorCron,
      handlers: {
        processTick: queue.statusesCheck
      }
    });
    statusesCheck = statusesCheckCronJob.run;
  } else {
    statusesCheck = function() {
      return Promise.resolve().then(queue.statusesCheck);
    };
  }

  return {
    enqueue: queue.enqueue,
    processNextBatch: processCronJob.run,
    cleanup: cleanup,
    resetRecords: queue.resetRecords,
    statusesCheck: statusesCheck
  };
};

module.exports.skip = utils.skip;
module.exports.fail = utils.fail;
