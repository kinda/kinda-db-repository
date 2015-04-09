"use strict";

var _ = require('lodash');
var KindaObject = require('kinda-object');

var KindaDBRepository = KindaObject.extend('KindaDBRepository', function() {
  this.isLocal = true; // TODO: improve this

  this.setCreator(function(database) {
    this.setDatabase(database);
  });

  this.getDatabase = function() {
    var database = this._database;
    if (!database) throw new Error('undefined database');
    return database;
  };

  this.setDatabase = function(database) {
    this._database = database;
  };

  this.getItem = function *(item, options) {
    var name = item.getCollection().getName();
    var key = item.getPrimaryKeyValue();
    var json = yield this.getDatabase().getItem(name, key, options);
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
    json = yield this.getDatabase().putItem(name, key, json, options);
    if (json) item.replaceValue(json);
  };

  this.deleteItem = function *(item, options) {
    var name = item.getCollection().getName();
    var key = item.getPrimaryKeyValue();
    yield this.getDatabase().deleteItem(name, key, options);
  };

  this.getItems = function *(items, options) {
    if (!items.length) return [];
    // we suppose that every items are part of the same collection:
    var collection = items[0].getCollection();
    var name = collection.getName();
    var keys = _.invoke(items, 'getPrimaryKeyValue');
    var items = yield this.getDatabase().getItems(name, keys, options);
    items = items.map(function(item) {
      // TODO: like getItem(), try to reuse the passed items instead of
      // build new one
      return collection.unserializeItem(item.value);
    });
    return items;
  };

  this.findItems = function *(collection, options) {
    var name = collection.getName();
    var items = yield this.getDatabase().findItems(name, options);
    items = items.map(function(item) {
      return collection.unserializeItem(item.value);
    });
    return items;
  };

  this.countItems = function *(collection, options) {
    var name = collection.getName();
    return yield this.getDatabase().countItems(name, options);
  };

  this.forEachItems = function *(collection, options, fn, thisArg) {
    var name = collection.getName();
    yield this.getDatabase().forEachItems(name, options, function *(value) {
      var item = collection.unserializeItem(value);
      yield fn.call(thisArg, item);
    });
  };
});

module.exports = KindaDBRepository;
