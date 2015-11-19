"use strict";
// http://docs.sequelizejs.com/en/latest/
var Sequelize = require("sequelize");
var _ = require("underscore");
var async = require("async");

var breakPromise = function(promise, callback) {
  promise.then(function(i) {
    return setTimeout(function() { callback(null, i); }, 1);
  }, function(e) {
    if (e.message === "ER_EMPTY_QUERY: Query was empty") return callback();
    return setTimeout(function() { callback(e); }, 1);
  });
};

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

  self.ready = true;
  self.populate(function() { });
};

SqlStore.prototype.populate = function(callback) {
  var self = this;
  breakPromise(self.sequelize.sync({ force: true }), function(err) {
    if (err) throw err;

    async.map(self.resourceConfig.examples, function(exampleJson, asyncCallback) {
      self.create({ request: { type: self.resourceConfig.resource } }, exampleJson, asyncCallback);
    }, callback);
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

SqlStore.prototype._generateSearchBlock = function(request) {
  return this._recurseOverSearchBlock(request.params.filter);
};

SqlStore.prototype._recurseOverSearchBlock = function(obj) {
  var self = this;
  if (!obj) return { };
  var searchBlock = { };
  // console.log("?", obj)
  Object.keys(obj).forEach(function(attributeName) {
    var textToMatch = obj[attributeName];
    if (textToMatch instanceof Array) {
      searchBlock[attributeName] = { $or: textToMatch.map(self._recurseOverSearchBlock) };
    } else if (textToMatch instanceof Object) {
      // Do nothing, its a nested filter
    } else if (textToMatch[0] === ">") {
      searchBlock[attributeName] = { $gt: textToMatch.substring(1) };
    } else if (textToMatch[0] === "<") {
      searchBlock[attributeName] = { $lt: textToMatch.substring(1) };
    } else if (textToMatch[0] === "~") {
      searchBlock[attributeName] = { $like: textToMatch.substring(1) };
    } else if (textToMatch[0] === ":") {
      searchBlock[attributeName] = { $like: "%" + textToMatch.substring(1) + "%" };
    } else {
      searchBlock[attributeName] = textToMatch;
    }
  });

  return searchBlock;
};


/**
  Search for a list of resources, given a resource type.
 */
SqlStore.prototype.search = function(request, callback) {
  var self = this;

  breakPromise(self.baseModel.findAndCount({
    where: self._generateSearchBlock(request),
    include: self._filterInclude(request.params.relationships, self.relations),
    limit: undefined,
    offset: undefined
  }), function(err, result) {
    if (err) return self._errorHandler(err, callback);

    var records = result.rows.map(function(i){ return self._fixObject(i.toJSON()); });
    return callback(null, records/*, result.count*/);
  });
};

/**
  Find a specific resource, given a resource type and and id.
 */
SqlStore.prototype.find = function(request, callback) {
  var self = this;

  breakPromise(self.baseModel.findOne({
    where: { id: request.params.id },
    include: self.relationArray
  }), function(err, theResource) {
    if (err) return self._errorHandler(err, callback);

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
  });
};

/**
  Create (store) a new resource give a resource type and an object.
 */
SqlStore.prototype.create = function(request, newResource, finishedCallback) {
  var self = this;

  breakPromise(self.sequelize.transaction(), function(err1, transaction) {
    var t = { transaction: transaction };
    if (err1) return self._errorHandler(err1, finishedCallback);

    breakPromise(self.baseModel.create(newResource, t), function(err2, baseInstance) {
      if (err2) return self._errorHandler(err2, finishedCallback);

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
              breakPromise(relationModel.create(item, t), function(err, newRelationModel) {
                if (err) return callback(err);

                baseInstance["add" + uc](newRelationModel);
                return callback(null, newRelationModel);
              });
            }, taskCallback);
          } else {
            breakPromise(relationModel.create(prop, t), function(err, newRelationModel) {
              if (err) return taskCallback(err);

              baseInstance["set" + uc](newRelationModel);
              return taskCallback(null, newRelationModel);
            });
          }
        };
      });

      async.parallel(tasks, function(err) {
        if (err) return finishedCallback(err);

        breakPromise(transaction.commit(), function(err3) {
          if (err3) return finishedCallback(err3);

          return finishedCallback(null, newResource);
        });
      });
    });
  });
};

/**
  Delete a resource, given a resource type and and id.
 */
SqlStore.prototype.delete = function(request, callback) {
  var self = this;

  breakPromise(self.baseModel.findAll({
    where: { id: request.params.id },
    include: self.relationArray
  }), function(findErr, results) {
    if (findErr) return self._errorHandler(findErr, callback);

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

    breakPromise(theResource.destroy(), function(deleteErr) {
      return callback(deleteErr);
    });
  });
};

/**
  Update a resource, given a resource type and id, along with a partialResource.
  partialResource contains a subset of changes that need to be merged over the original.
 */
SqlStore.prototype.update = function(request, partialResource, finishedCallback) {
  var self = this;

  var transactionOptions = {
    isolationLevel: Sequelize.Transaction.ISOLATION_LEVELS.READ_UNCOMMITTED,
    autocommit: false
  };
  breakPromise(self.sequelize.transaction(transactionOptions), function(err1, transaction) {
    if (err1) return self._errorHandler(err1, finishedCallback);

    var t = { transaction: transaction };
    var failCallback = function(e) {
      var a = function() {
        self._errorHandler(e, finishedCallback);
      };
      transaction.rollback().then(a, a);
    };

    breakPromise(self.baseModel.findAll({
      where: { id: request.params.id },
      include: self.relationArray,
      transaction: t.transaction
    }), function(err2, results) {
      if (err2) return failCallback(err2);

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
              deadRow.destroy(t);
            });

            async.map(prop, function(item, acallback) {
              breakPromise(relationModel.create(item, t), function(err4, newRelationModel) {
                if (err4) return acallback(err4);

                breakPromise(theResource["add" + uc](newRelationModel, t), acallback);
              });
            }, taskCallback);
          } else {
            if (theResource[keyName]) {
              theResource[keyName].destroy(t);
            }
            if (!prop) {
              breakPromise(theResource["set" + uc](null, t), taskCallback);
            } else {
              breakPromise(relationModel.create(prop, t), function(err3, newRelationModel) {
                if (err3) return taskCallback(err3);

                breakPromise(theResource["set" + uc](newRelationModel, t), taskCallback);
              });
            }
          }
        };
      });

      async.parallel(tasks, function(err) {
        if (err) return failCallback(err);

        breakPromise(theResource.update(partialResource, t), function(err4) {
          if (err4) return failCallback(err4);

          breakPromise(transaction.commit(), function(err3) {
            if (err3) return finishedCallback(err3);

            return finishedCallback(null, null);
          });
        });
      });
    });
  });
};
