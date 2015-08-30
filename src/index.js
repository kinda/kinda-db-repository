'use strict';

let _ = require('lodash');
let idgen = require('idgen');
let util = require('kinda-util').create();
let KindaAbstractRepository = require('kinda-abstract-repository');
let KindaObjectDB = require('kinda-object-db');

const VERSION = 1;
const RESPIRATION_RATE = 250;

let KindaLocalRepository = KindaAbstractRepository.extend('KindaLocalRepository', function() {
  this.isLocal = true; // TODO: improve this

  let superCreator = this.creator;
  this.creator = function(app, options) {
    if (_.isPlainObject(app)) {
      options = app;
      app = undefined;
    }
    if (!options) options = {};
    superCreator.call(this, app, options);

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

  this.initializeRepository = async function() {
    if (this.hasBeenInitialized) return;
    if (this.isInitializing) return;
    if (this.isInsideTransaction) {
      throw new Error('cannot initialize the repository inside a transaction');
    }
    this.isInitializing = true;
    try {
      await this.objectDatabase.initializeObjectDatabase();
      let hasBeenCreated = await this.createRepositoryIfDoesNotExist();
      if (!hasBeenCreated) {
        await this.objectDatabase.database.lockDatabase();
        try {
          await this.upgradeRepository();
        } finally {
          await this.objectDatabase.database.unlockDatabase();
        }
      }
      this.hasBeenInitialized = true;
      await this.emit('didInitialize');
    } finally {
      this.isInitializing = false;
    }
  };

  this.loadRepositoryRecord = async function(tr = this.store, errorIfMissing = true) {
    await this.initializeRepository();
    return await tr.get([this.name, '$Repository'], { errorIfMissing });
  };

  this.saveRepositoryRecord = async function(record, tr = this.store, errorIfExists) {
    await tr.put([this.name, '$Repository'], record, {
      errorIfExists,
      createIfMissing: !errorIfExists
    });
  };

  this.createRepositoryIfDoesNotExist = async function() {
    let hasBeenCreated = false;
    await this.store.transaction(async function(tr) {
      let record = await this.loadRepositoryRecord(tr, false);
      if (!record) {
        record = {
          name: this.name,
          version: VERSION,
          id: idgen(16)
        };
        await this.saveRepositoryRecord(record, tr, true);
        hasBeenCreated = true;
        await this.emit('didCreate');
        this.log.info(`Repository '${this.name}' created`);
      }
    }.bind(this));
    return hasBeenCreated;
  };

  this.upgradeRepository = async function() {
    let record = await this.loadRepositoryRecord();
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
    await this.saveRepositoryRecord(record);
    this.log.info(`Repository '${this.name}' upgraded to version ${VERSION}`);

    this.emit('upgradeDidStop');
  };

  this.destroyRepository = async function() {
    await this.emit('willDestroy');
    await this.objectDatabase.destroyObjectDatabase();
    this.hasBeenInitialized = false;
    delete this.repository._repositoryId;
    await this.emit('didDestroy');
  };

  this.getRepositoryId = async function() {
    if (this._repositoryId) return this._repositoryId;
    let record = await this.loadRepositoryRecord();
    this.repository._repositoryId = record.id;
    return record.id;
  };

  this.transaction = async function(fn, options) {
    if (this.isInsideTransaction) return await fn(this);
    await this.initializeRepository();
    return await this.objectDatabase.transaction(async function(tr) {
      let transaction = Object.create(this);
      transaction.objectDatabase = tr;
      return await fn(transaction);
    }.bind(this), options);
  };

  Object.defineProperty(this, 'isInsideTransaction', {
    get() {
      return this !== this.repository;
    }
  });

  // === Operations ====

  this.getItem = async function(item, options) {
    let className = item.class.name;
    let key = item.primaryKeyValue;
    await this.initializeRepository();
    let result = await this.objectDatabase.getItem(className, key, options);
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

  this.putItem = async function(item, options = {}) {
    let classNames = item.classNames;
    let key = item.primaryKeyValue;
    let json = item.serialize();
    options = _.clone(options);
    if (item.isNew) options.errorIfExists = true;
    await this.initializeRepository();
    await this.objectDatabase.putItem(classNames, key, json, options);
    await this.emit('didPutItem', item, options);
  };

  this.deleteItem = async function(item, options) {
    let className = item.class.name;
    let key = item.primaryKeyValue;
    await this.initializeRepository();
    let hasBeenDeleted = await this.objectDatabase.deleteItem(
      className, key, options
    );
    if (hasBeenDeleted) await this.emit('didDeleteItem', item, options);
    return hasBeenDeleted;
  };

  this.getItems = async function(items, options) {
    if (!items.length) return [];
    // we suppose that every items are part of the same collection:
    let className = items[0].class.name;
    let keys = _.pluck(items, 'primaryKeyValue');
    let iterationsCount = 0;
    await this.initializeRepository();
    let results = await this.objectDatabase.getItems(className, keys, options);
    let cache = {};
    let finalItems = [];
    for (let result of results) {
      // TODO: like getItem(), try to reuse the passed items instead of
      // build new one
      let resultClassName = result.classes[0];
      let realCollection = this.createCollectionFromItemClassName(resultClassName, cache);
      finalItems.push(realCollection.unserializeItem(result.value));
      if (++iterationsCount % RESPIRATION_RATE === 0) await util.timeout(0);
    }
    return finalItems;
  };

  this.findItems = async function(collection, options) {
    let className = collection.Item.name;
    let iterationsCount = 0;
    await this.initializeRepository();
    let results = await this.objectDatabase.findItems(className, options);
    let cache = {};
    let items = [];
    for (let result of results) {
      let resultClassName = result.classes[0];
      let realCollection = this.createCollectionFromItemClassName(resultClassName, cache);
      items.push(realCollection.unserializeItem(result.value));
      if (++iterationsCount % RESPIRATION_RATE === 0) await util.timeout(0);
    }
    return items;
  };

  this.countItems = async function(collection, options) {
    let className = collection.Item.name;
    await this.initializeRepository();
    return await this.objectDatabase.countItems(className, options);
  };

  this.forEachItems = async function(collection, options, fn, thisArg) {
    let className = collection.Item.name;
    let cache = {};
    await this.initializeRepository();
    await this.objectDatabase.forEachItems(className, options, async function(result) {
      let resultClassName = result.classes[0];
      let realCollection = this.createCollectionFromItemClassName(resultClassName, cache);
      let item = realCollection.unserializeItem(result.value);
      await fn.call(thisArg, item);
    }, this);
  };

  this.findAndDeleteItems = async function(collection, options) {
    let deletedItemsCount = 0;
    await this.forEachItems(collection, options, async function(item) {
      let hasBeenDeleted = await item.delete({ errorIfMissing: false });
      if (hasBeenDeleted) deletedItemsCount++;
    }, this);
    return deletedItemsCount;
  };
});

module.exports = KindaLocalRepository;
