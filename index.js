"use strict";

var _ = require('lodash');
var KindaAbstractRepository = require('kinda-abstract-repository');
var KindaObjectDB = require('kinda-object-db');

var KindaLocalRepository = KindaAbstractRepository.extend('KindaLocalRepository', function() {
  this.isLocal = true; // TODO: improve this

  var superCreator = this.getCreator();
  this.setCreator(function(name, url, collectionClasses, options) {
    superCreator.apply(this, arguments);
    var classes = [];
    collectionClasses.forEach(function(klass) {
      var collectionPrototype = klass.getPrototype();
      var itemClass = collectionPrototype.Item;
      var itemPrototype = itemClass.getPrototype();
      classes.push({
        name: itemClass.getName(),
        indexes: itemPrototype.getIndexes()
      });
    }, this);
    this.database = KindaObjectDB.create(name, url, classes);
  });

  this.setDatabase = function(database) {
    this._database = database;
  };

  this.getItem = function *(item, options) {
    var className = item.getClassName();
    var key = item.getPrimaryKeyValue();
    var result = yield this.database.getItem(className, key, options);
    if (!result) return; // means item is not found and errorIfMissing is false
    var resultClassName = result.classes[0];
    if (resultClassName === className) {
      item.replaceValue(result.value);
    } else {
      var collection = this.createCollectionFromItemClassName(resultClassName);
      item = collection.unserializeItem(result.value);
    }
    return item;
  };

  this.putItem = function *(item, options) {
    var classNames = [];
    item.getSuperclasses().forEach(function(superclass) {
      var prototype = superclass.getPrototype();
      if (!prototype.getPrimaryKeyProperty) return;
      if (!prototype.getPrimaryKeyProperty(false)) return;
      classNames.push(superclass.getName());
    });
    classNames.unshift(item.getClassName());
    classNames = _.uniq(classNames);
    var key = item.getPrimaryKeyValue();
    var json = item.serialize();
    options = _.clone(options);
    if (item.isNew) options.errorIfExists = true;
    yield this.database.putItem(classNames, key, json, options);
  };

  this.deleteItem = function *(item, options) {
    var className = item.getClassName();
    var key = item.getPrimaryKeyValue();
    yield this.database.deleteItem(className, key, options);
  };

  this.getItems = function *(items, options) {
    if (!items.length) return [];
    // we suppose that every items are part of the same collection:
    var className = items[0].getClassName();
    var keys = _.invoke(items, 'getPrimaryKeyValue');
    var results = yield this.database.getItems(className, keys, options);
    var cache = {};
    var items = results.map(function(result) {
      // TODO: like getItem(), try to reuse the passed items instead of
      // build new one
      var resultClassName = result.classes[0];
      var collection = this.createCollectionFromItemClassName(resultClassName, cache);
      return collection.unserializeItem(result.value);
    }, this);
    return items;
  };

  this.findItems = function *(collection, options) {
    var className = collection.Item.getName();
    var results = yield this.database.findItems(className, options);
    var cache = {};
    var items = results.map(function(result) {
      var resultClassName = result.classes[0];
      var collection = this.createCollectionFromItemClassName(resultClassName, cache);
      return collection.unserializeItem(result.value);
    }, this);
    return items;
  };

  this.countItems = function *(collection, options) {
    var className = collection.Item.getName();
    return yield this.database.countItems(className, options);
  };

  this.forEachItems = function *(collection, options, fn, thisArg) {
    var className = collection.Item.getName();
    var cache = {};
    yield this.database.forEachItems(className, options, function *(result) {
      var resultClassName = result.classes[0];
      var collection = this.createCollectionFromItemClassName(resultClassName, cache);
      var item = collection.unserializeItem(result.value);
      yield fn.call(thisArg, item);
    }, this);
  };

  this.findAndDeleteItems = function *(collection, options) {
    yield this.forEachItems(collection, options, function *(item) {
      yield this.deleteItem(item, { errorIfMissing: false });
    }, this);
  };

  this.transaction = function *(fn, options) {
    if (this.isInsideTransaction()) return yield fn(this);
    return yield this.database.transaction(function *(tr) {
      var transaction = Object.create(this);
      transaction.database = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };

  this.isInsideTransaction = function() {
    return this !== this.repository;
  };
});

module.exports = KindaLocalRepository;
