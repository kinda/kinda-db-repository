"use strict";

var _ = require('lodash');
var idgen = require('idgen');
var log = require('kinda-log').create();
var KindaAbstractRepository = require('kinda-abstract-repository');
var KindaObjectDB = require('kinda-object-db');

var VERSION = 1;

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

    this.objectDatabase = KindaObjectDB.create(name, url, classes);

    this.objectDatabase.on('upgradeDidStart', function() {
      this.emit('upgradeDidStart');
    }.bind(this))

    this.objectDatabase.on('upgradeDidStop', function() {
      this.emit('upgradeDidStop');
    }.bind(this))

    this.objectDatabase.on('migrationDidStart', function() {
      this.emit('migrationDidStart');
    }.bind(this))

    this.objectDatabase.on('migrationDidStop', function() {
      this.emit('migrationDidStop');
    }.bind(this))
  });

  this.getStore = function() {
    return this.objectDatabase.getStore();
  };

  this.initializeRepository = function *() {
    if (this.hasBeenInitialized) return;
    if (this.isInitializing) return;
    if (this.isInsideTransaction()) {
      throw new Error('cannot initialize the repository inside a transaction');
    }
    this.isInitializing = true;
    try {
      yield this.objectDatabase.initializeObjectDatabase();
      var hasBeenCreated = yield this.createRepositoryIfDoesNotExist();
      if (!hasBeenCreated) {
        yield this.objectDatabase.database.lockDatabase();
        try {
          yield this.upgradeRepository();
        } finally {
          yield this.objectDatabase.database.unlockDatabase();
        }
      }
      this.hasBeenInitialized = true;
      yield this.emitAsync('didInitialize');
    } finally {
      this.isInitializing = false;
    }
  };

  this.loadRepositoryRecord = function *(tr, errorIfMissing) {
    if (!tr) tr = this.getStore();
    yield this.initializeRepository();
    if (errorIfMissing == null) errorIfMissing = true;
    return yield tr.get([this.name, '$Repository'], { errorIfMissing: errorIfMissing });
  };

  this.saveRepositoryRecord = function *(record, tr, errorIfExists) {
    if (!tr) tr = this.getStore();
    yield tr.put([this.name, '$Repository'], record, {
      errorIfExists: errorIfExists,
      createIfMissing: !errorIfExists
    });
  };

  this.createRepositoryIfDoesNotExist = function *(tr) {
    var hasBeenCreated = false;
    yield this.getStore().transaction(function *(tr) {
      var record = yield this.loadRepositoryRecord(tr, false);
      if (!record) {
        record = {
          name: this.name,
          version: VERSION,
          id: idgen(16)
        };
        yield this.saveRepositoryRecord(record, tr, true);
        hasBeenCreated = true;
        yield this.emitAsync('didCreate');
        log.info("Repository '" + this.name + "' created");
      }
    }.bind(this));
    return hasBeenCreated;
  };

  this.upgradeRepository = function *() {
    var record = yield this.loadRepositoryRecord();
    var version = record.version;

    if (version === VERSION) return;

    if (version > VERSION) {
      throw new Error('cannot downgrade the object database');
    }

    this.emit('upgradeDidStart');

    if (version < 2) {
      // ...
    }

    record.version = VERSION;
    yield this.saveRepositoryRecord(record);
    log.info("Repository '" + this.name + "' upgraded to version " + VERSION);

    this.emit('upgradeDidStop');
  };

  this.destroyRepository = function *() {
    yield this.objectDatabase.destroyObjectDatabase();
  };

  this.getRepositoryId = function *() {
    if (this._repositoryId) return this._repositoryId;
    var record = yield this.loadRepositoryRecord();
    this.repository._repositoryId = record.id;
    return record.id;
  };

  this.transaction = function *(fn, options) {
    if (this.isInsideTransaction()) return yield fn(this);
    yield this.initializeRepository();
    return yield this.objectDatabase.transaction(function *(tr) {
      var transaction = Object.create(this);
      transaction.objectDatabase = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };

  this.isInsideTransaction = function() {
    return this !== this.repository;
  };

  // === Operations ====

  this.getItem = function *(item, options) {
    var className = item.getClassName();
    var key = item.getPrimaryKeyValue();
    yield this.initializeRepository();
    var result = yield this.objectDatabase.getItem(className, key, options);
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
    var classNames = item.getClassNames();
    var key = item.getPrimaryKeyValue();
    var json = item.serialize();
    options = _.clone(options);
    if (item.isNew) options.errorIfExists = true;
    yield this.initializeRepository();
    yield this.objectDatabase.putItem(classNames, key, json, options);
    yield this.emitAsync('didPutItem', item, options);
  };

  this.deleteItem = function *(item, options) {
    var className = item.getClassName();
    var key = item.getPrimaryKeyValue();
    yield this.initializeRepository();
    yield this.objectDatabase.deleteItem(className, key, options);
    yield this.emitAsync('didDeleteItem', item, options);
  };

  this.getItems = function *(items, options) {
    if (!items.length) return [];
    // we suppose that every items are part of the same collection:
    var className = items[0].getClassName();
    var keys = _.invoke(items, 'getPrimaryKeyValue');
    yield this.initializeRepository();
    var results = yield this.objectDatabase.getItems(className, keys, options);
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
    yield this.initializeRepository();
    var results = yield this.objectDatabase.findItems(className, options);
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
    yield this.initializeRepository();
    return yield this.objectDatabase.countItems(className, options);
  };

  this.forEachItems = function *(collection, options, fn, thisArg) {
    var className = collection.Item.getName();
    var cache = {};
    yield this.initializeRepository();
    yield this.objectDatabase.forEachItems(className, options, function *(result) {
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
});

module.exports = KindaLocalRepository;
