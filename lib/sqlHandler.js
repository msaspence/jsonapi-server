"use strict";
// http://docs.sequelizejs.com/en/latest/
var Sequelize = require("sequelize");
var _ = require("underscore");

var SqlStore = module.exports = function SqlStore(config) {
  this.config = config;
};

/**
  Handlers readiness status. This should be set to `true` once all handlers are ready to process requests.
 */
SqlStore.prototype.ready = false;

/**
  initialise gets invoked once for each resource that uses this hander.
  In this instance, we're allocating an array in our in-memory data store.
 */
SqlStore.prototype.initialise = function(resourceConfig) {
  var self = this;

  self.sequelize = new Sequelize(resourceConfig.resource, self.config.username, self.config.password);

  self._buildModels();

  self.sequelize.sync().then(function() {
    self.ready = true;
    console.log("Sync'd and ready");
  });
};

SqlStore.prototype._buildModels = function() {
  var self = this;
  self.models = { };

  var localAttributes = Object.keys(self.resourceConfig.attributes).filter(function(attributeName) {
    var settings = self.resourceConfig.attributes[attributeName]._settings;
    return !(settings.__one || settings.__many);
  });
  localAttributes = _.pick(self.resourceConfig.attributes, localAttributes);
  var relations = Object.keys(self.resourceConfig.attributes).filter(function(attributeName) {
    var settings = self.resourceConfig.attributes[attributeName]._settings;
    return (settings.__one || settings.__many);
  });
  relations = _.pick(self.resourceConfig.attributes, relations);

  var modelAttributes = self._joiSchemaToSequelizeModel(localAttributes);
  var baseModel = self.sequelize.define(self.resourceConfig.resource, modelAttributes);
  self.models[self.resourceConfig.resource] = baseModel;

  relations.forEach(function(relation) {

  });
};

SqlStore.prototype._joiSchemaToSequelizeModel = function(joiSchema) {
  var model = { };

  Object.keys(joiSchema).forEach(function(attributeName) {
    var attribute = joiSchema[attributeName];
    console.log(attribute)
  });

  return model;
};

SqlStore.prototype._defineRelationModel = function(relationName, many) {
  var self = this;

  var modelName = self.resourceConfig.resource + "-" + relationName;
  var modelProperties = {
    relation: {
      type: DataTypes.STRING
    },
    meta: {
      type: DataTypes.STRING
    }
  };

  if (many) {
    modelProperties = _.extend(modelProperties, {
      id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true
      },
      relationId: {
        type: DataTypes.STRING,
        references: self.resourceConfig.resource,
        referencesKey: relationName
      }
    });
  } else {
    modelProperties = _.extend(modelProperties, {
      relationId: {
        type: DataTypes.STRING,
        primaryKey: true
      }
    });
  }

  return self.sequelize.define(modelName, modelProperties);
};


/**
  Search for a list of resources, give a resource type.
 */
SqlStore.prototype.search = function(request, callback) {
  var self = this;
  return callback(self);
};

/**
  Find a specific resource, given a resource type and and id.
 */
SqlStore.prototype.find = function(request, callback) {
  var self = this;
  return callback(self);
};

/**
  Create (store) a new resource give a resource type and an object.
 */
SqlStore.prototype.create = function(request, newResource, callback) {
  var self = this;
  return callback(self);
};

/**
  Delete a resource, given a resource type and and id.
 */
SqlStore.prototype.delete = function(request, callback) {
  var self = this;
  return callback(self);
};

/**
  Update a resource, given a resource type and id, along with a partialResource.
  partialResource contains a subset of changes that need to be merged over the original.
 */
SqlStore.prototype.update = function(request, partialResource, callback) {
  var self = this;
  return callback(self);
};
