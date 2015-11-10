"use strict";
var mongoStore = module.exports = { };
mongoStore.handlers = { };

var _ = require("underscore");
// resources represents out in-memory data store
var resources = { };

var mongodb = require('mongodb');
var db;


/**
  initialise gets invoked once for each resource that uses this hander.
  In this instance, we're allocating an array in our in-memory data store.
 */
mongoStore.handlers.initialise = function(resourceConfig) {
  mongodb.MongoClient.connect('mongodb://localhost:27017/jsonapi', function(err, database) {
    if (err) return console.error('ERROR!');
    console.log('Connected correctly to server:', database);
    db = database;
  });
  resources[resourceConfig.resource] = resourceConfig.examples || [ ];
};

/**
  Search for a list of resources, give a resource type.
 */
mongoStore.handlers.search = function(request, callback) {
  // If a relationships param is passed in, filter against those relations
  if (request.params.relationships) {
    var mustMatch = request.params.relationships;
    var matches = resources[request.params.type].filter(function(anyResource) {
      var match = true;
      Object.keys(mustMatch).forEach(function(i) {
        var fKeys = anyResource[i];
        if (!(fKeys instanceof Array)) fKeys = [ fKeys ];
        fKeys = fKeys.map(function(j) { return j.id; });
        if (fKeys.indexOf(mustMatch[i]) === -1) {
          match = false;
        }
      });
      return match;
    });
    return callback(null, matches);
  }

  // No specific search params are supported, so return ALL resources of the requested type
  return callback(null, resources[request.params.type]);
};

/**
  Find a specific resource, given a resource type and and id.
 */
mongoStore.handlers.find = function(request, callback) {
  var collection = db.collection(request.params.type);
  var documentId = new mongodb.Binary(request.params.id, mongodb.Binary.SUBTYPE_UUID);
  collection.findOne({ _id: documentId }, function(err, result) {
    if (err || !result) {
      return callback({
        status: "404",
        code: "ENOTFOUND",
        title: "Requested resource does not exist",
        detail: "There is no " + request.params.type + " with id " + request.params.id
      });
    }
    var theResource = _.omit(result, '_id');
    return callback(null, theResource);
  });
};

/**
  Create (store) a new resource give a resource type and an object.
 */
mongoStore.handlers.create = function(request, newResource, callback) {
  var document = _.clone(newResource);
  document._id = new mongodb.Binary(document.id, mongodb.Binary.SUBTYPE_UUID);
  var collection = db.collection(document.type);
  collection.insertOne(document, function(err, result) {
    if (err) return callback(err);
    return callback(null, newResource);
  });
};

/**
  Delete a resource, given a resource type and an id.
 */
mongoStore.handlers.delete = function(request, callback) {
  var document = { _id: new mongodb.Binary(request.params.id, mongodb.Binary.SUBTYPE_UUID) };
  var collection = db.collection(request.params.type);
  collection.deleteOne(document, function(err, result) {
    if (err) return callback(err);
    return callback(null);
  });
};

/**
  Update a resource, given a resource type and id, along with a partialResource.
  partialResource contains a subset of changes that need to be merged over the original.
 */
mongoStore.handlers.update = function(request, partialResource, callback) {
  var collection = db.collection(request.params.type);
  var documentId = new mongodb.Binary(request.params.id, mongodb.Binary.SUBTYPE_UUID);
  collection.updateOne({ _id: documentId }, { $set: partialResource }, function(err, result) {
    if (err) return callback(err);

    return mongoStore.handlers.find(request, callback);
  });
};
