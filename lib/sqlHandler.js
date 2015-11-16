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
  self.resourceConfig = resourceConfig;

  self.sequelize = new Sequelize(resourceConfig.resource, self.config.username, self.config.password);

  self._buildModels();

  self.sequelize.sync({ force: true, logging: console.log }).then(function() {
    self.ready = true;
    console.log("Sync'd and ready");
  });
};

SqlStore.prototype._buildModels = function() {
  var self = this;
  self.models = { };

  var localAttributes = Object.keys(self.resourceConfig.attributes).filter(function(attributeName) {
    var settings = self.resourceConfig.attributes[attributeName]._settings;
    if (!settings) return true;
    return !(settings.__one || settings.__many);
  });
  localAttributes = _.pick(self.resourceConfig.attributes, localAttributes);
  var relations = Object.keys(self.resourceConfig.attributes).filter(function(attributeName) {
    var settings = self.resourceConfig.attributes[attributeName]._settings;
    if (!settings) return false;
    return (settings.__one || settings.__many);
  });
  relations = _.pick(self.resourceConfig.attributes, relations);

  var modelAttributes = self._joiSchemaToSequelizeModel(localAttributes);
  var baseModel = self.sequelize.define(self.resourceConfig.resource, modelAttributes, { timestamps: false });
  self.models[self.resourceConfig.resource] = baseModel;

  Object.keys(relations).forEach(function(relationName) {
    var relation = relations[relationName];
    self.models["." + relationName] = self._defineRelationModel(relationName, relation._settings.__many);
  });
};

SqlStore.prototype._joiSchemaToSequelizeModel = function(joiSchema) {
  var model = {
    id: { type: new Sequelize.STRING(38), primaryKey: true },
    type: Sequelize.STRING
  };

  Object.keys(joiSchema).forEach(function(attributeName) {
    var attribute = joiSchema[attributeName];
    if (attribute._type === "string") model[attributeName] = Sequelize.STRING;
    if (attribute._type === "number") model[attributeName] = Sequelize.INTEGER;
  });

  return model;
};

SqlStore.prototype._defineRelationModel = function(relationName, many) {
  var self = this;

  var modelName = self.resourceConfig.resource + "-" + relationName;
  var modelProperties = {
    relation: {
      type: new Sequelize.STRING(38),
      allowNull: false
    },
    meta: {
      type: Sequelize.STRING
    }
  };

  if (many) {
    modelProperties = _.extend(modelProperties, {
      id: {
        type: Sequelize.INTEGER,
        primaryKey: true,
        autoIncrement: true
      },
      relationId: {
        type: new Sequelize.STRING(38),
        references: {
          model: self.resourceConfig.resource,
          key: "id"
        }
      }
    });
  } else {
    modelProperties = _.extend(modelProperties, {
      relationId: {
        type: new Sequelize.STRING(38),
        primaryKey: true
      }
    });
  }

  return self.sequelize.define(modelName, modelProperties, { timestamps: false });
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
