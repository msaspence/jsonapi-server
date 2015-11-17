"use strict";
// http://docs.sequelizejs.com/en/latest/
var Sequelize = require("sequelize");
var _ = require("underscore");
var async = require("async");

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

    (resourceConfig.examples || [ ]).forEach(function(i) {
      self.create({ request: { type: resourceConfig.resource } }, i, function() { });
    });

  });
};

SqlStore.prototype._buildModels = function() {
  var self = this;

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
  self.baseModel = self.sequelize.define(self.resourceConfig.resource, modelAttributes, { timestamps: false });

  self.relations = { };
  self.relationArray = [ ];
  Object.keys(relations).forEach(function(relationName) {
    var relation = relations[relationName];
    var otherModel = self._defineRelationModel(relationName, relation._settings.__many);
    self.relations[relationName] = otherModel;
    self.relationArray.push(otherModel);
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
    uid: {
      type: Sequelize.INTEGER,
      primaryKey: true,
      autoIncrement: true
    },
    id: {
      type: new Sequelize.STRING(38),
      allowNull: false
    },
    type: {
      type: new Sequelize.STRING(38),
      allowNull: false
    },
    meta: {
      type: Sequelize.STRING,
      get: function() {
        var data = this.getDataValue("meta");
        if (!data) return undefined;
        return JSON.parse(data);
      },
      set: function(val) {
        this.setDataValue("meta", JSON.stringify(val));
      }
    }
  };

  var relatedModel = self.sequelize.define(modelName, modelProperties, { timestamps: false });

  if (many) {
    self.baseModel.hasMany(relatedModel);
  } else {
    self.baseModel.hasOne(relatedModel);
  }

  return relatedModel;
};


/**
  Search for a list of resources, give a resource type.
 */
SqlStore.prototype.search = function(request, callback) {
  var self = this;

  self.baseModel.findAll({
    where: {

    },
    include: self.relationArray
  }).then(function(results) {
    results = results.map(function(i){ return self._fixObject(i.toJSON()); });
    return callback(null, results);
  });
};

/**
  Find a specific resource, given a resource type and and id.
 */
SqlStore.prototype.find = function(request, callback) {
  var self = this;

  self.baseModel.findAll({
    where: {
      id: request.params.id
    },
    include: self.relationArray
  }).then(function(results) {

    results = self._fixObject(results[0].toJSON());
    return callback(null, results);
  });
};

/**
  Create (store) a new resource give a resource type and an object.
 */
SqlStore.prototype.create = function(request, newResource, finishedCallback) {
  var self = this;

  self.baseModel.create(newResource).then(function(baseInstance) {

    var tasks = { };
    Object.keys(self.relations).forEach(function(relationName) {
      var prop = newResource[relationName];
      var relationModel = self.relations[relationName];

      var keyName = self.resourceConfig.resource + "-" + relationName;
      var uc = keyName[0].toUpperCase() + keyName.slice(1, keyName.length);
      if (!prop) return;

      tasks[relationName] = function(taskCallback) {
        if (prop instanceof Array) {
          async.map(prop, function(item, callback) {
            relationModel.create(item).then(function(i) {
              baseInstance["add" + uc](i);
              return callback(null, i);
            }, function(e) { console.error("ERR", e); return callback(e); });
          }, taskCallback);
        } else {
          relationModel.create(prop).then(function(i) {
            baseInstance["set" + uc](i);
            return taskCallback(null, i);
          }, function(e) { console.error("ERR", e); return taskCallback(e); });
        }
      };
    });

    async.parallel(tasks, function(err) {
      if (err) return finishedCallback(err);
      baseInstance.save().then(function() {
        return finishedCallback(null, newResource);
      }, function(finalErr) {
        return finishedCallback(finalErr);
      });
    });
  });
};

/**
  Delete a resource, given a resource type and and id.
 */
SqlStore.prototype.delete = function(request, callback) {
  var self = this;

  self.baseModel.findAll({
    where: {
      id: request.params.id
    },
    include: self.relationArray
  }).then(function(results) {

    results = results[0];

    results.destroy().then(callback, callback);
  });
};

/**
  Update a resource, given a resource type and id, along with a partialResource.
  partialResource contains a subset of changes that need to be merged over the original.
 */
SqlStore.prototype.update = function(request, partialResource, callback) {
  var self = this;
  return callback(self);
};

SqlStore.prototype._fixObject = function(json) {
  var self = this;
  var bleh = self.resourceConfig.resource;
  if (bleh[bleh.length - 1] === "s") bleh = bleh.substring(0, bleh.length - 1);
  bleh += "Id";

  Object.keys(json).forEach(function(attribute) {
    if (attribute.indexOf(self.resourceConfig.resource + "-") !== 0) return;

    var fixedName = attribute.split(self.resourceConfig.resource + "-").pop();
    json[fixedName] = json[attribute];

    var val = json[attribute];
    if (!(val instanceof Array)) val = [ val ];

    val.forEach(function(j) {
      if (j.uid) delete j.uid;
      if (j[bleh]) delete j[bleh];
    });
    delete json[attribute];

  });

  return json;
};
