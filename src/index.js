'use strict';

let _ = require('lodash');
let idgen = require('idgen');
let KindaAbstractRepository = require('kinda-abstract-repository');
let KindaObjectDB = require('kinda-object-db');

const VERSION = 1;

let KindaLocalRepository = KindaAbstractRepository.extend('KindaLocalRepository', function() {
  this.isLocal = true; // TODO: improve this

  let superCreator = this.creator;
  this.creator = function(application, options) {
    if (_.isPlainObject(application)) {
      options = application;
      application = undefined;
    }
    if (!options) options = {};
    superCreator.call(this, application, options);

    let classes = [];
    _.forOwn(this.collectionClasses, klass => {
      let collectionPrototype = klass.prototype;
      let itemClass = collectionPrototype.Item;
      let itemPrototype = itemClass.prototype;
      classes.push({
        name: itemClass.name,
        indexes: itemPrototype.indexes
      });
    });

    this.objectDatabase = KindaObjectDB.create({
      name: options.name,
      url: options.url,
      classes
    });

    this.objectDatabase.on('upgradeDidStart', () => this.emit('upgradeDidStart'));
    this.objectDatabase.on('upgradeDidStop', () => this.emit('upgradeDidStop'));
    this.objectDatabase.on('migrationDidStart', () => this.emit('migrationDidStart'));
    this.objectDatabase.on('migrationDidStop', () => this.emit('migrationDidStop'));
  };

  Object.defineProperty(this, 'store', {
    get() {
      return this.objectDatabase.store;
    }
  });

  this.initializeRepository = function *() {
    if (this.hasBeenInitialized) return;
    if (this.isInitializing) return;
    if (this.isInsideTransaction) {
      throw new Error('cannot initialize the repository inside a transaction');
    }
    this.isInitializing = true;
    try {
      yield this.objectDatabase.initializeObjectDatabase();
      let hasBeenCreated = yield this.createRepositoryIfDoesNotExist();
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

  this.loadRepositoryRecord = function *(tr = this.store, errorIfMissing = true) {
    yield this.initializeRepository();
    return yield tr.get([this.name, '$Repository'], { errorIfMissing });
  };

  this.saveRepositoryRecord = function *(record, tr = this.store, errorIfExists) {
    yield tr.put([this.name, '$Repository'], record, {
      errorIfExists,
      createIfMissing: !errorIfExists
    });
  };

  this.createRepositoryIfDoesNotExist = function *() {
    let hasBeenCreated = false;
    yield this.store.transaction(function *(tr) {
      let record = yield this.loadRepositoryRecord(tr, false);
      if (!record) {
        record = {
          name: this.name,
          version: VERSION,
          id: idgen(16)
        };
        yield this.saveRepositoryRecord(record, tr, true);
        hasBeenCreated = true;
        yield this.emitAsync('didCreate');
        this.log.info(`Repository '${this.name}' created`);
      }
    }.bind(this));
    return hasBeenCreated;
  };

  this.upgradeRepository = function *() {
    let record = yield this.loadRepositoryRecord();
    let version = record.version;

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
    this.log.info(`Repository '${this.name}' upgraded to version ${VERSION}`);

    this.emit('upgradeDidStop');
  };

  this.destroyRepository = function *() {
    yield this.emitAsync('willDestroy');
    yield this.objectDatabase.destroyObjectDatabase();
    this.hasBeenInitialized = false;
    delete this.repository._repositoryId;
    yield this.emitAsync('didDestroy');
  };

  this.getRepositoryId = function *() {
    if (this._repositoryId) return this._repositoryId;
    let record = yield this.loadRepositoryRecord();
    this.repository._repositoryId = record.id;
    return record.id;
  };

  this.transaction = function *(fn, options) {
    if (this.isInsideTransaction) return yield fn(this);
    yield this.initializeRepository();
    return yield this.objectDatabase.transaction(function *(tr) {
      let transaction = Object.create(this);
      transaction.objectDatabase = tr;
      return yield fn(transaction);
    }.bind(this), options);
  };

  Object.defineProperty(this, 'isInsideTransaction', {
    get() {
      return this !== this.repository;
    }
  });

  // === Operations ====

  this.getItem = function *(item, options) {
    let className = item.class.name;
    let key = item.primaryKeyValue;
    yield this.initializeRepository();
    let result = yield this.objectDatabase.getItem(className, key, options);
    if (!result) return undefined; // means item is not found and errorIfMissing is false
    let resultClassName = result.classes[0];
    if (resultClassName === className) {
      item.replaceValue(result.value);
    } else {
      let realCollection = this.createCollectionFromItemClassName(resultClassName);
      item = realCollection.unserializeItem(result.value);
    }
    return item;
  };

  this.putItem = function *(item, options = {}) {
    let classNames = item.classNames;
    let key = item.primaryKeyValue;
    let json = item.serialize();
    options = _.clone(options);
    if (item.isNew) options.errorIfExists = true;
    yield this.initializeRepository();
    yield this.objectDatabase.putItem(classNames, key, json, options);
    yield this.emitAsync('didPutItem', item, options);
  };

  this.deleteItem = function *(item, options) {
    let className = item.class.name;
    let key = item.primaryKeyValue;
    yield this.initializeRepository();
    let hasBeenDeleted = yield this.objectDatabase.deleteItem(
      className, key, options
    );
    if (hasBeenDeleted) yield this.emitAsync('didDeleteItem', item, options);
    return hasBeenDeleted;
  };

  this.getItems = function *(items, options) {
    if (!items.length) return [];
    // we suppose that every items are part of the same collection:
    let className = items[0].class.name;
    let keys = _.pluck(items, 'primaryKeyValue');
    yield this.initializeRepository();
    let results = yield this.objectDatabase.getItems(className, keys, options);
    let cache = {};
    items = results.map(result => {
      // TODO: like getItem(), try to reuse the passed items instead of
      // build new one
      let resultClassName = result.classes[0];
      let realCollection = this.createCollectionFromItemClassName(resultClassName, cache);
      return realCollection.unserializeItem(result.value);
    });
    return items;
  };

  this.findItems = function *(collection, options) {
    let className = collection.Item.name;
    yield this.initializeRepository();
    let results = yield this.objectDatabase.findItems(className, options);
    let cache = {};
    let items = results.map(result => {
      let resultClassName = result.classes[0];
      let realCollection = this.createCollectionFromItemClassName(resultClassName, cache);
      return realCollection.unserializeItem(result.value);
    });
    return items;
  };

  this.countItems = function *(collection, options) {
    let className = collection.Item.name;
    yield this.initializeRepository();
    return yield this.objectDatabase.countItems(className, options);
  };

  this.forEachItems = function *(collection, options, fn, thisArg) {
    let className = collection.Item.name;
    let cache = {};
    yield this.initializeRepository();
    yield this.objectDatabase.forEachItems(className, options, function *(result) {
      let resultClassName = result.classes[0];
      let realCollection = this.createCollectionFromItemClassName(resultClassName, cache);
      let item = realCollection.unserializeItem(result.value);
      yield fn.call(thisArg, item);
    }, this);
  };

  this.findAndDeleteItems = function *(collection, options) {
    let deletedItemsCount = 0;
    yield this.forEachItems(collection, options, function *(item) {
      let hasBeenDeleted = yield item.delete({ errorIfMissing: false });
      if (hasBeenDeleted) deletedItemsCount++;
    }, this);
    return deletedItemsCount;
  };
});

module.exports = KindaLocalRepository;
