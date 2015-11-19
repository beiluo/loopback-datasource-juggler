var async = require('async');
var utils = require('./utils');
var List = require('./list');
var isPlainObject = utils.isPlainObject;
var defineCachedRelations = utils.defineCachedRelations;
var ObjectID = require("mongodb").ObjectID;
var uniq = utils.uniq;


/**
 * Utility Function to allow interleave before and after high computation tasks
 * @param tasks
 * @param callback
 */
function execTasksWithInterLeave(tasks, callback) {
  //let's give others some time to process.
  //Context Switch BEFORE Heavy Computation
  process.nextTick(function () {
    //Heavy Computation
    async.parallel(tasks, function (err, info) {
      //Context Switch AFTER Heavy Computation
      process.nextTick(function () {
        callback(err, info);
      });
    });
  });
}
function idName(m) {
  return m.definition.idName() || 'id';
}
function properties(model) {
  var project = {};
  Object.keys(model.definition.rawProperties).forEach(function (key) {
    project[key] = true;
  })

  return project;
}
/*!
 * Include mixin for ./model.js
 */
module.exports = Inclusion;

/**
 * Inclusion - Model mixin.
 *
 * @class
 */

function Inclusion() {
}
/**
 * @param {Array} objects 当前数据
 * @param {Object} multi 关联关系
 * @param {Object} [options] filter 查询条件
 * @param {Function} cb 回调函数
 *
 */
Inclusion.multiPointer = function (objects, modeName, includefilter, multiPointer, callback) {
  includefilter = includefilter || {};
  multiPointer = multiPointer || {};
  var self = this;
  var multi = multiPointer[modeName];
  if (!isPlainObject(multi) || !objects || objects.length == 0) {
    return process.nextTick(function () {
      callback();
    });
  }

  var foreignKey = multi["foreignKey"];
  var targertKey = multi["targertKey"];
  var targertClass = multi["targertClass"];
  var destKey = multi["destKey"]||foreignKey;
  if (!foreignKey || !targertKey || !targertClass) {
    return process.nextTick(function () {
      callback();
    });
  }
  var models = self.modelBuilder.models;
  var sourceIds = [];
  var objIdMap = {};
  var aggregate_query_terms = [];
  var query_terms = [];
  var pk_name = idName(models[modeName]);
  var fk_name = idName(models[targertClass]);
  var raw_properties = properties(models[targertClass]);
  objects.forEach(function (obj) {
    var sourceId = obj[foreignKey];
    if (sourceId) {
      sourceIds.push(sourceId.toString());
      objIdMap["id:" + sourceId.toString()] = obj;
      var query = [];
      if (Array.isArray(obj[foreignKey])) {
        query = query.concat(obj[foreignKey] || []);
      } else {
        if (pk_name == foreignKey) {
          query = [String(obj[foreignKey])];
        } else {
          query = [obj[foreignKey]]
        }
      }
      if (fk_name === targertKey) {
        for (var i = 0, len = query.length; i < len; i++) {
          query[i] = ObjectID(query[i]);
        }
      }
      aggregate_query_terms.push(["id:" + sourceId].concat(query));
      query_terms = query_terms.concat(query);
    }
    defineCachedRelations(obj);
    obj.__cachedRelations[foreignKey] = [];
  })
  var filter = includefilter[targertClass] || {};
  filter.where = filter.where || {};
  filter.where[targertKey] = {
    inq: uniq(query_terms)
  };

  var aggregate = [];

  aggregate.push({'where': filter.where});//$match
  raw_properties["id:id"] = {
    "$map": {
      "input": aggregate_query_terms,
      "as": "grade",
      "in": {"$cond": [{"$setIsSubset": [['$' + targertKey], "$$grade"]}, {"$slice": ["$$grade", 0, 1]}, null]}
    }
  }
  aggregate.push({
    "fields": raw_properties
  })
  aggregate.push({"expand": "$id:id"});
  aggregate.push({"expand": "$id:id"});//需要解构2次
  aggregate.push({"where": {"id:id": {"ne": null}}})

  if (filter.order) {
    aggregate.push({"order": filter.order});
  }
  aggregate.push({
    "group": {
      "_id": "$id:id",
      "rows": {"$push": "$$ROOT"}
    }
  });
  var skip = 0, limit = 100;//增加限制
  if (filter.skip) {
    skip = filter.skip;
    limit += skip;
  }
  if (filter.limit) {
    limit = filter.limit;
  }
  aggregate.push({
    "fields": {
      "_id": 1,
      "nrows": {"$slice": ["$rows", skip, limit]}
    }
  })

  models[targertClass].aggregate(aggregate, targetFetchHandler)
  function targetFetchHandler(err, targets) {
    if (err) {
      return callback(err);
    }
    var tasks = [];
    var tmp = [];
    targets.forEach(function (target) {
      tmp = tmp.concat(target["nrows"]);
    })
    targets = tmp;
    //simultaneously process subIncludes
    if (multiPointer[targertClass] && targets) {
      tasks.push(function subIncludesTask(next) {
        models[targertClass].multiPointer(targets, targertClass, includefilter, multiPointer, next);
      });
    }
    //process each target object
    tasks.push(targetLinkingTask);
    function targetLinkingTask(next) {
      if (targets.length === 0) {
        return async.each(objects, function (obj, next) {
          processTargetObj(obj, next);
        }, next);
      }
      async.each(targets, linkManyToOne, next);
      function linkManyToOne(target, next) {
        var targetIds = [].concat(target["id:id"]);
        async.map(targetIds, function (targetId, next) {
          var obj = objIdMap[targetId];
          if (!obj) return next();
          obj.__cachedRelations[foreignKey].push(target);
          processTargetObj(obj, next);
        }, function (err, processedTargets) {
          if (err) {
            return next(err);
          }

          var objsWithEmptyRelation = processedTargets.filter(function (obj) {
            return obj.__cachedRelations[foreignKey].length === 0;
          });
          async.each(objsWithEmptyRelation, function (obj, next) {
            processTargetObj(obj, next);
          }, function (err) {
            next(err, processedTargets);
          });
        });
      }
    }

    execTasksWithInterLeave(tasks, callback);
  }

  function processTargetObj(obj, callback) {
    var inst = (obj instanceof self) ? obj : new self(obj);

    function setIncludeData(result, cb) {
      if (obj === inst) {
        if (Array.isArray(result) && !(result instanceof List)) {
          result = new List(result, models[targertClass]);
        }
        obj.__data[destKey] = result;
        obj.setStrict(false);
      } else {
        obj[destKey] = result;
      }
      cb(null, obj);
    }

    if (obj.__cachedRelations &&
      obj.__cachedRelations[foreignKey] !== undefined) {
      return setIncludeData(obj.__cachedRelations[foreignKey],
        callback);
    }
  }
};

