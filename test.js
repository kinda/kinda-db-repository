'use strict';

require('co-mocha');
let assert = require('chai').assert;
let _ = require('lodash');
let Collection = require('kinda-collection');
let KindaLocalRepository = require('./src');

suite('KindaLocalRepository', function() {
  let repository, accounts, people, companies;

  let catchError = function *(fn) {
    let err;
    try {
      yield fn();
    } catch (e) {
      err = e;
    }
    return err;
  };

  suiteSetup(function *() {
    let Elements = Collection.extend('Elements', function() {
      this.Item = this.Item.extend('Element', function() {
        this.addPrimaryKeyProperty('id', String);
      });
    });

    let Accounts = Elements.extend('Accounts', function() {
      this.Item = this.Item.extend('Account', function() {
        this.addProperty('accountNumber', Number);
        this.addProperty('country', String);
        this.addIndex('accountNumber');
        this.addIndex('country');
      });
    });

    let People = Elements.extend('People', function() {
      this.include(Accounts);
      this.Item = this.Item.extend('Person', function() {
        this.addProperty('firstName', String);
        this.addProperty('lastName', String);
        this.addIndex(['lastName', 'firstName']);
      });
    });

    let Companies = Elements.extend('Companies', function() {
      this.include(Accounts);
      this.Item = this.Item.extend('Company', function() {
        this.addProperty('name', String);
        this.addIndex('name');
      });
    });

    repository = KindaLocalRepository.create({
      name: 'Test',
      url: 'mysql://test@localhost/test',
      collections: [Elements, Accounts, People, Companies]
    });

    accounts = repository.createCollection('Accounts');
    people = repository.createCollection('People');
    companies = repository.createCollection('Companies');
  });

  suiteTeardown(function *() {
    yield repository.destroyRepository();
  });

  test('get root collection class', function *() {
    let klass = repository.rootCollectionClass;
    assert.strictEqual(klass.name, 'Elements');
  });

  test('repository id', function *() {
    let id = yield repository.getRepositoryId();
    assert.ok(id);
  });

  test('put, get and delete some items', function *() {
    let mvila = people.createItem({
      accountNumber: 12345,
      firstName: 'Manuel',
      lastName: 'Vila',
      country: 'France'
    });
    yield mvila.save();
    let id = mvila.id;
    let item = yield people.getItem(id);
    assert.strictEqual(item.accountNumber, 12345);
    assert.strictEqual(item.firstName, 'Manuel');
    let hasBeenDeleted = yield item.delete();
    assert.isTrue(hasBeenDeleted);
    item = yield people.getItem(id, { errorIfMissing: false });
    assert.isUndefined(item);
  });

  test('get a missing item', function *() {
    let err = yield catchError(function *() {
      yield people.getItem('xyz');
    });
    assert.instanceOf(err, Error);

    let item = yield people.getItem('xyz', { errorIfMissing: false });
    assert.isUndefined(item);
  });

  test('put an already existing item', function *() {
    let jack = people.createItem({ firstName: 'Jack', country: 'USA' });
    yield jack.save();
    let err = yield catchError(function *() {
      let jack2 = people.createItem({ id: jack.id, firstName: 'Jack', country: 'UK' });
      yield jack2.save();
    });
    assert.instanceOf(err, Error);

    yield jack.delete();
  });

  test('delete a missing item', function *() {
    let err = yield catchError(function *() {
      yield people.deleteItem('xyz');
    });
    assert.instanceOf(err, Error);

    let hasBeenDeleted;
    err = yield catchError(function *() {
      hasBeenDeleted = yield people.deleteItem('xyz', { errorIfMissing: false });
    });
    assert.isFalse(hasBeenDeleted);
    assert.isUndefined(err);
  });

  suite('with many items', function() {
    setup(function *() {
      yield accounts.putItem({
        id: 'aaa',
        accountNumber: 45329,
        country: 'France'
      });
      yield people.putItem({
        id: 'bbb',
        accountNumber: 3246,
        firstName: 'Jack',
        lastName: 'Daniel',
        country: 'USA'
      });
      yield companies.putItem({
        id: 'ccc',
        accountNumber: 7002,
        name: 'Kinda Ltd',
        country: 'China'
      });
      yield people.putItem({
        id: 'ddd',
        accountNumber: 55498,
        firstName: 'Vincent',
        lastName: 'Vila',
        country: 'USA'
      });
      yield people.putItem({
        id: 'eee',
        accountNumber: 888,
        firstName: 'Pierre',
        lastName: 'Dupont',
        country: 'France'
      });
      yield companies.putItem({
        id: 'fff',
        accountNumber: 8775,
        name: 'Fleur SARL',
        country: 'France'
      });
    });

    teardown(function *() {
      yield accounts.deleteItem('aaa', { errorIfMissing: false });
      yield accounts.deleteItem('bbb', { errorIfMissing: false });
      yield accounts.deleteItem('ccc', { errorIfMissing: false });
      yield accounts.deleteItem('ddd', { errorIfMissing: false });
      yield accounts.deleteItem('eee', { errorIfMissing: false });
      yield accounts.deleteItem('fff', { errorIfMissing: false });
    });

    test('get many items by id', function *() {
      let items = yield accounts.getItems(['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].class.name, 'Account');
      assert.strictEqual(items[0].id, 'aaa');
      assert.strictEqual(items[0].accountNumber, 45329);
      assert.deepEqual(items[1].class.name, 'Company');
      assert.strictEqual(items[1].id, 'ccc');
      assert.strictEqual(items[1].accountNumber, 7002);
    });

    test('find all items in a collection', function *() {
      let items = yield companies.findItems();
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].class.name, 'Company');
      assert.strictEqual(items[0].id, 'ccc');
      assert.strictEqual(items[0].name, 'Kinda Ltd');
      assert.strictEqual(items[1].class.name, 'Company');
      assert.strictEqual(items[1].id, 'fff');
      assert.strictEqual(items[1].name, 'Fleur SARL');
    });

    test('find and order items', function *() {
      let items = yield people.findItems({ order: 'accountNumber' });
      assert.strictEqual(items.length, 3);
      let numbers = _.pluck(items, 'accountNumber');
      assert.deepEqual(numbers, [888, 3246, 55498]);
    });

    test('find items with a query', function *() {
      let items = yield accounts.findItems({ query: { country: 'USA' } });
      let ids = _.pluck(items, 'id');
      assert.deepEqual(ids, ['bbb', 'ddd']);

      items = yield companies.findItems({ query: { country: 'UK' } });
      assert.strictEqual(items.length, 0);
    });

    test('count all items in a collection', function *() {
      let count = yield people.countItems();
      assert.strictEqual(count, 3);
    });

    test('count items with a query', function *() {
      let count = yield accounts.countItems({ query: { country: 'France' } });
      assert.strictEqual(count, 3);

      count = yield people.countItems({ query: { country: 'France' } });
      assert.strictEqual(count, 1);

      count = yield companies.countItems({ query: { country: 'Spain' } });
      assert.strictEqual(count, 0);
    });

    test('iterate over items', function *() {
      let ids = [];
      yield accounts.forEachItems({ batchSize: 2 }, function *(item) {
        ids.push(item.id);
      });
      assert.deepEqual(ids, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    test('find and delete items', function *() {
      let options = { query: { country: 'France' }, batchSize: 2 };
      let deletedItemsCount = yield accounts.findAndDeleteItems(options);
      assert.strictEqual(deletedItemsCount, 3);
      let items = yield accounts.findItems();
      let ids = _.pluck(items, 'id');
      assert.deepEqual(ids, ['bbb', 'ccc', 'ddd']);
      deletedItemsCount = yield accounts.findAndDeleteItems(options);
      assert.strictEqual(deletedItemsCount, 0);
    });

    test('change an item inside a transaction', function *() {
      let item = yield people.getItem('bbb');
      assert.strictEqual(item.lastName, 'Daniel');
      assert.isFalse(item.isInsideTransaction);
      yield item.transaction(function *(newItem) {
        assert.isTrue(newItem.isInsideTransaction);
        newItem.lastName = 'D.';
        yield newItem.save();
        let loadedItem = yield newItem.collection.getItem('bbb');
        assert.strictEqual(loadedItem.lastName, 'D.');
      });
      assert.strictEqual(item.lastName, 'D.');
      item = yield people.getItem('bbb');
      assert.strictEqual(item.lastName, 'D.');
    });

    test('change an item inside an aborted transaction', function *() {
      let item = yield people.getItem('bbb');
      assert.strictEqual(item.lastName, 'Daniel');
      let err = yield catchError(function *() {
        assert.isFalse(item.isInsideTransaction);
        yield item.transaction(function *(newItem) {
          assert.isTrue(newItem.isInsideTransaction);
          newItem.lastName = 'D.';
          yield newItem.save();
          let loadedItem = yield newItem.collection.getItem('bbb');
          assert.strictEqual(loadedItem.lastName, 'D.');
          throw new Error('something is wrong');
        });
      });
      assert.instanceOf(err, Error);
      assert.strictEqual(item.lastName, 'Daniel');
      item = yield people.getItem('bbb');
      assert.strictEqual(item.lastName, 'Daniel');
    });
  }); // 'with many items' suite
});
