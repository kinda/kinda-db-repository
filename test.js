'use strict';

let assert = require('chai').assert;
let _ = require('lodash');
let Collection = require('kinda-collection');
let KindaLocalRepository = require('./src');

suite('KindaLocalRepository', function() {
  let repository, accounts, people, companies;

  let catchError = async function(fn) {
    let err;
    try {
      await fn();
    } catch (e) {
      err = e;
    }
    return err;
  };

  suiteSetup(async function() {
    let Elements = Collection.extend('Elements', function() {
      this.Item = this.Item.extend('Element', function() {
        this.addPrimaryKeyProperty('id', String);
        this.addCreatedOnProperty('createdOn');
        this.addIndex('createdOn');
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

  suiteTeardown(async function() {
    await repository.destroyRepository();
  });

  test('get root collection class', async function() {
    let klass = repository.rootCollectionClass;
    assert.strictEqual(klass.name, 'Elements');
  });

  test('repository id', async function() {
    let id = await repository.getRepositoryId();
    assert.ok(id);
  });

  test('put, get and delete some items', async function() {
    let mvila = people.createItem({
      accountNumber: 12345,
      firstName: 'Manuel',
      lastName: 'Vila',
      country: 'France'
    });
    assert.isUndefined(mvila.createdOn);
    await mvila.save();
    assert.isDefined(mvila.createdOn);
    let id = mvila.id;
    let item = await people.getItem(id);
    assert.strictEqual(item.accountNumber, 12345);
    assert.strictEqual(item.firstName, 'Manuel');
    let hasBeenDeleted = await item.delete();
    assert.isTrue(hasBeenDeleted);
    item = await people.getItem(id, { errorIfMissing: false });
    assert.isUndefined(item);
  });

  test('get a missing item', async function() {
    let err = await catchError(async function() {
      await people.getItem('xyz');
    });
    assert.instanceOf(err, Error);

    let item = await people.getItem('xyz', { errorIfMissing: false });
    assert.isUndefined(item);
  });

  test('put an already existing item', async function() {
    let jack = people.createItem({ firstName: 'Jack', country: 'USA' });
    await jack.save();
    let err = await catchError(async function() {
      let jack2 = people.createItem({ id: jack.id, firstName: 'Jack', country: 'UK' });
      await jack2.save();
    });
    assert.instanceOf(err, Error);

    await jack.delete();
  });

  test('delete a missing item', async function() {
    let err = await catchError(async function() {
      await people.deleteItem('xyz');
    });
    assert.instanceOf(err, Error);

    let hasBeenDeleted;
    err = await catchError(async function() {
      hasBeenDeleted = await people.deleteItem('xyz', { errorIfMissing: false });
    });
    assert.isFalse(hasBeenDeleted);
    assert.isUndefined(err);
  });

  suite('with many items', function() {
    setup(async function() {
      await accounts.putItem({
        id: 'aaa',
        accountNumber: 45329,
        country: 'France'
      });
      await people.putItem({
        id: 'bbb',
        accountNumber: 3246,
        firstName: 'Jack',
        lastName: 'Daniel',
        country: 'USA'
      });
      await companies.putItem({
        id: 'ccc',
        accountNumber: 7002,
        name: 'Kinda Ltd',
        country: 'China'
      });
      await people.putItem({
        id: 'ddd',
        accountNumber: 55498,
        firstName: 'Vincent',
        lastName: 'Vila',
        country: 'USA'
      });
      await people.putItem({
        id: 'eee',
        accountNumber: 888,
        firstName: 'Pierre',
        lastName: 'Dupont',
        country: 'France'
      });
      await companies.putItem({
        id: 'fff',
        accountNumber: 8775,
        name: 'Fleur SARL',
        country: 'France'
      });
    });

    teardown(async function() {
      await accounts.deleteItem('aaa', { errorIfMissing: false });
      await accounts.deleteItem('bbb', { errorIfMissing: false });
      await accounts.deleteItem('ccc', { errorIfMissing: false });
      await accounts.deleteItem('ddd', { errorIfMissing: false });
      await accounts.deleteItem('eee', { errorIfMissing: false });
      await accounts.deleteItem('fff', { errorIfMissing: false });
    });

    test('get many items by id', async function() {
      let items = await accounts.getItems(['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].class.name, 'Account');
      assert.strictEqual(items[0].id, 'aaa');
      assert.strictEqual(items[0].accountNumber, 45329);
      assert.deepEqual(items[1].class.name, 'Company');
      assert.strictEqual(items[1].id, 'ccc');
      assert.strictEqual(items[1].accountNumber, 7002);
    });

    test('find all items in a collection', async function() {
      let items = await companies.findItems();
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].class.name, 'Company');
      assert.strictEqual(items[0].id, 'ccc');
      assert.strictEqual(items[0].name, 'Kinda Ltd');
      assert.strictEqual(items[1].class.name, 'Company');
      assert.strictEqual(items[1].id, 'fff');
      assert.strictEqual(items[1].name, 'Fleur SARL');
    });

    test('find and order items', async function() {
      let items = await people.findItems({ order: 'accountNumber' });
      assert.strictEqual(items.length, 3);
      let numbers = _.pluck(items, 'accountNumber');
      assert.deepEqual(numbers, [888, 3246, 55498]);
    });

    test('find items with a query', async function() {
      let items = await accounts.findItems({ query: { country: 'USA' } });
      let ids = _.pluck(items, 'id');
      assert.deepEqual(ids, ['bbb', 'ddd']);

      items = await companies.findItems({ query: { country: 'UK' } });
      assert.strictEqual(items.length, 0);
    });

    test('count all items in a collection', async function() {
      let count = await people.countItems();
      assert.strictEqual(count, 3);
    });

    test('count items with a query', async function() {
      let count = await accounts.countItems({ query: { country: 'France' } });
      assert.strictEqual(count, 3);

      count = await people.countItems({ query: { country: 'France' } });
      assert.strictEqual(count, 1);

      count = await companies.countItems({ query: { country: 'Spain' } });
      assert.strictEqual(count, 0);
    });

    test('iterate over items', async function() {
      let ids = [];
      await accounts.forEachItems({ batchSize: 2 }, async function(item) {
        ids.push(item.id);
      });
      assert.deepEqual(ids, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    test('find and delete items', async function() {
      let options = { query: { country: 'France' }, batchSize: 2 };
      let deletedItemsCount = await accounts.findAndDeleteItems(options);
      assert.strictEqual(deletedItemsCount, 3);
      let items = await accounts.findItems();
      let ids = _.pluck(items, 'id');
      assert.deepEqual(ids, ['bbb', 'ccc', 'ddd']);
      deletedItemsCount = await accounts.findAndDeleteItems(options);
      assert.strictEqual(deletedItemsCount, 0);
    });

    test('change an item inside a transaction', async function() {
      let item = await people.getItem('bbb');
      assert.strictEqual(item.lastName, 'Daniel');
      assert.isFalse(item.isInsideTransaction);
      await item.transaction(async function(newItem) {
        assert.isTrue(newItem.isInsideTransaction);
        newItem.lastName = 'D.';
        await newItem.save();
        let loadedItem = await newItem.collection.getItem('bbb');
        assert.strictEqual(loadedItem.lastName, 'D.');
      });
      assert.strictEqual(item.lastName, 'D.');
      item = await people.getItem('bbb');
      assert.strictEqual(item.lastName, 'D.');
    });

    test('change an item inside an aborted transaction', async function() {
      let item = await people.getItem('bbb');
      assert.strictEqual(item.lastName, 'Daniel');
      let err = await catchError(async function() {
        assert.isFalse(item.isInsideTransaction);
        await item.transaction(async function(newItem) {
          assert.isTrue(newItem.isInsideTransaction);
          newItem.lastName = 'D.';
          await newItem.save();
          let loadedItem = await newItem.collection.getItem('bbb');
          assert.strictEqual(loadedItem.lastName, 'D.');
          throw new Error('something is wrong');
        });
      });
      assert.instanceOf(err, Error);
      assert.strictEqual(item.lastName, 'Daniel');
      item = await people.getItem('bbb');
      assert.strictEqual(item.lastName, 'Daniel');
    });
  }); // 'with many items' suite
});
