"use strict";

var _ = require('lodash');
var KindaObject = require('kinda-object');
var KindaDB = require('kinda-db');

var KindaLocalRepository = KindaObject.extend('KindaLocalRepository', function() {
  this.isLocal = true; // TODO: improve this

  this.setCreator(function(name, url, collections, options) {
    if (!name) throw new Error('name is missing');
    if (!url) throw new Error('url is missing');
    if (!_.isArray(collections)) throw new Error('collections parameter is invalid');
    if (!options) options = {};
    this.name = name;
    var tables = [];
    collections.forEach(function(collection) {
      var collectionPrototype = collection.getPrototype();
      collectionPrototype.setRepository(this);
      var itemPrototype = collectionPrototype.Item.getPrototype();
      var table = {
        name: collection.getName(),
        indexes: itemPrototype.getIndexes()
      };
      tables.push(table);
    }, this);
    this.database = KindaDB.create(name, url, tables);
    this.repository = this;
  });

  this.setDatabase = function(database) {
    this._database = database;
  };

  this.getItem = function *(item, options) {
    var name = item.getCollection().getName();
    var key = item.getPrimaryKeyValue();
    var json = yield this.database.getItem(name, key, options);
    if (!json) return;
    item.replaceValue(json);
    return item;
  };

  this.putItem = function *(item, options) {
    var name = item.getCollection().getName();
    var key = item.getPrimaryKeyValue();
    var json = item.serialize();
    options = _.clone(options);
    if (item.isNew) options.errorIfExists = true;
    json = yield this.database.putItem(name, key, json, options);
    if (json) item.replaceValue(json);
  };

  this.deleteItem = function *(item, options) {
    var name = item.getCollection().getName();
    var key = item.getPrimaryKeyValue();
    yield this.database.deleteItem(name, key, options);
  };

  this.getItems = function *(items, options) {
    if (!items.length) return [];
    // we suppose that every items are part of the same collection:
    var collection = items[0].getCollection();
    var name = collection.getName();
    var keys = _.invoke(items, 'getPrimaryKeyValue');
    var items = yield this.database.getItems(name, keys, options);
    items = items.map(function(item) {
      // TODO: like getItem(), try to reuse the passed items instead of
      // build new one
      return collection.unserializeItem(item.value);
    });
    return items;
  };

  this.findItems = function *(collection, options) {
    var name = collection.getName();
    var items = yield this.database.findItems(name, options);
    items = items.map(function(item) {
      return collection.unserializeItem(item.value);
    });
    return items;
  };

  this.countItems = function *(collection, options) {
    var name = collection.getName();
    return yield this.database.countItems(name, options);
  };

  this.forEachItems = function *(collection, options, fn, thisArg) {
    var name = collection.getName();
    yield this.database.forEachItems(name, options, function *(value) {
      var item = collection.unserializeItem(value);
      yield fn.call(thisArg, item);
    });
  };

  this.findAndDeleteItems = function *(collection, options) {
    yield this.forEachItems(collection, options, function *(item) {
      yield this.deleteItem(item, { errorIfMissing: false });
    }, this);
  };

  this.transaction = function *(fn, options) {
    if (this.repository !== this)
      return yield fn(this); // we are already in a transaction
    return yield this.database.transaction(function *(tr) {
      var transaction = Object.create(this);
      transaction.database = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };
});

module.exports = KindaLocalRepository;
