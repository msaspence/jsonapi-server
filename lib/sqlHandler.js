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

  self.sequelize = new Sequelize(resourceConfig.resource, self.config.username, self.config.password, {
    dialect: self.config.dialect,
    logging: self.config.logging || false
  });

  self._buildModels();

  self.sequelize.sync({
    force: true
  }).then(function() {
    self.ready = true;

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
    type: Sequelize.STRING,
    meta: {
      type: Sequelize.STRING,
      get: function() {
        var data = this.getDataValue("meta");
        if (!data) return undefined;
        return JSON.parse(data);
      },
      set: function(val) {
        return this.setDataValue("meta", JSON.stringify(val));
      }
    }
  };

  Object.keys(joiSchema).forEach(function(attributeName) {
    var attribute = joiSchema[attributeName];
    if (attribute._type === "string") model[attributeName] = { type: Sequelize.STRING, allowNull: true };
    if (attribute._type === "date") model[attributeName] = { type: Sequelize.STRING, allowNull: true };
    if (attribute._type === "number") model[attributeName] = { type: Sequelize.INTEGER, allowNull: true };
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
        return this.setDataValue("meta", JSON.stringify(val));
      }
    }
  };

  var relatedModel = self.sequelize.define(modelName, modelProperties, {
    timestamps: false,
    indexes: [ { fields: [ "id" ] } ],
    freezeTableName: true
  });

  if (many) {
    self.baseModel.hasMany(relatedModel, { onDelete: "CASCADE" });
  } else {
    self.baseModel.hasOne(relatedModel, { onDelete: "CASCADE" });
  }

  return relatedModel;
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
    delete json[attribute];
    if (!val) return;

    if (!(val instanceof Array)) val = [ val ];
    val.forEach(function(j) {
      if (j.uid) delete j.uid;
      if (j[bleh]) delete j[bleh];
    });
  });

  return json;
};

SqlStore.prototype._errorHandler = function(e, callback) {
  // console.log(e, e.stack);
  return callback({
    status: "500",
    code: "EUNKNOWN",
    title: "An unknown error has occured",
    detail: "Something broke when connecting to the database - " + e.message
  });
};

SqlStore.prototype._filterInclude = function(relationships, relations) {
  return Object.keys(relations).map(function(relationName) {
    var model = relations[relationName];
    if (!relationships || !relationships[relationName]) return model;
    return {
      model: model,
      where: { id: relationships[relationName] }
    };
  });
};

/**
  Search for a list of resources, give a resource type.
 */
SqlStore.prototype.search = function(request, callback) {
  var self = this;

  self.baseModel.findAll({
    where: {

    },
    include: self._filterInclude(request.params.relationships, self.relations)
  }).then(function(results) {
    results = results.map(function(i){ return self._fixObject(i.toJSON()); });
    return callback(null, results);
  }).catch(function(e) {
    return self._errorHandler(e, callback);
  });
};

/**
  Find a specific resource, given a resource type and and id.
 */
SqlStore.prototype.find = function(request, callback) {
  var self = this;

  self.baseModel.findAll({
    where: { id: request.params.id },
    include: self.relationArray
  }).then(function(results) {
    var theResource = results[0];

    // If the resource doesn't exist, error
    if (!theResource) {
      return callback({
        status: "404",
        code: "ENOTFOUND",
        title: "Requested resource does not exist",
        detail: "There is no " + request.params.type + " with id " + request.params.id
      });
    }

    theResource = self._fixObject(theResource.toJSON());
    return callback(null, theResource);
  }).catch(function(e) {
    return self._errorHandler(e, callback);
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
            }, function(e) { return callback(e); });
          }, taskCallback);
        } else {
          relationModel.create(prop).then(function(i) {
            baseInstance["set" + uc](i);
            return taskCallback(null, i);
          }, function(e) { return taskCallback(e); });
        }
      };
    });

    async.parallel(tasks, function(err) {
      if (err) return finishedCallback(err);
      return finishedCallback(null, newResource);
    });
  }).catch(function(e) {
    return self._errorHandler(e, finishedCallback);
  });
};

/**
  Delete a resource, given a resource type and and id.
 */
SqlStore.prototype.delete = function(request, callback) {
  var self = this;

  self.baseModel.findAll({
    where: { id: request.params.id },
    include: self.relationArray
  }).then(function(results) {
    var theResource = results[0];

    // If the resource doesn't exist, error
    if (!theResource) {
      return callback({
        status: "404",
        code: "ENOTFOUND",
        title: "Requested resource does not exist",
        detail: "There is no " + request.params.type + " with id " + request.params.id
      });
    }

    return theResource.destroy();
  }).then(callback.bind(null, null)).catch(function(e) {
    return self._errorHandler(e, callback);
  });
};

/**
  Update a resource, given a resource type and id, along with a partialResource.
  partialResource contains a subset of changes that need to be merged over the original.
 */
SqlStore.prototype.update = function(request, partialResource, finishedCallback) {
  var self = this;

  self.baseModel.findAll({
    where: { id: request.params.id },
    include: self.relationArray
  }).then(function(results) {
    var theResource = results[0];

    // If the resource doesn't exist, error
    if (!theResource) {
      return finishedCallback({
        status: "404",
        code: "ENOTFOUND",
        title: "Requested resource does not exist",
        detail: "There is no " + request.params.type + " with id " + request.params.id
      });
    }

    var tasks = { };
    Object.keys(self.relations).forEach(function(relationName) {
      var prop = partialResource[relationName];
      if (!partialResource.hasOwnProperty(relationName)) return;
      var relationModel = self.relations[relationName];

      var keyName = self.resourceConfig.resource + "-" + relationName;
      var uc = keyName[0].toUpperCase() + keyName.slice(1, keyName.length);

      tasks[relationName] = function(taskCallback) {
        if (prop instanceof Array) {
          (theResource[keyName] || []).map(function(deadRow) {
            deadRow.destroy();
          });
          theResource["set" + uc]([]).then(function() {
            async.map(prop, function(item, acallback) {
              relationModel.create(item).then(function(i) {
                theResource["add" + uc](i);
                return acallback(null, i);
              }, function(e) { return acallback(e); });
            }, taskCallback);
          });
        } else {
          if (theResource[keyName]) {
            theResource[keyName].destroy();
          }
          if (!prop) {
            theResource["set" + uc](null).then(function() {
              return taskCallback(null, null);
            }, function(e) { return taskCallback(e); });
          } else {
            relationModel.create(prop).then(function(i) {
              theResource["set" + uc](i).then(function() {
                return taskCallback(null, i);
              }, function(e) { return taskCallback(e); });
            }, function(e) { return taskCallback(e); });
          }
        }
      };
    });

    if (Object.keys(_.omit(partialResource, self.relations)).length > 2) {
      tasks.__base__ = function(taskCallback) {
        theResource.update(partialResource).then(function(i) {
          return taskCallback(null, i);
        }, function(e) {
          if (e.message === "ER_EMPTY_QUERY: Query was empty") return taskCallback(null, theResource);
          return taskCallback(e);
        });
      };
    }

    async.parallel(tasks, function(err) {
      if (err) return finishedCallback(err);
      return finishedCallback(null, null);
    });
  }).catch(function(e) {
    return self._errorHandler(e, finishedCallback);
  });
};


SqlStore.prototype._sanitiseSearch = function(params) {
  // { $gt: 6 }
  // $gte: 6,
  // $lt: 10,
  // $lte: 10,
  return params;
};
