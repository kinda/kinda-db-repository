"use strict";

require('co-mocha');
var assert = require('chai').assert;
var _ = require('lodash');
var Collection = require('kinda-collection');
var KindaLocalRepository = require('./');

suite('KindaLocalRepository', function() {
  var repository, accounts, people, companies;

  var catchError = function *(fn) {
    var err;
    try {
      yield fn();
    } catch (e) {
      err = e
    }
    return err;
  };

  suiteSetup(function *() {
    var Elements = Collection.extend('Elements', function() {
      this.Item = this.Item.extend('Element', function() {
        this.addPrimaryKeyProperty('id', String);
      });
    });

    var Accounts = Elements.extend('Accounts', function() {
      this.Item = this.Item.extend('Account', function() {
        this.addProperty('accountNumber', Number);
        this.addProperty('country', String);
        this.addIndex('accountNumber');
        this.addIndex('country');
      });
    });

    var People = Elements.extend('People', function() {
      this.include(Accounts);
      this.Item = this.Item.extend('Person', function() {
        this.addProperty('firstName', String);
        this.addProperty('lastName', String);
        this.addIndex(['lastName', 'firstName']);
      });
    });

    var Companies = Elements.extend('Companies', function() {
      this.include(Accounts);
      this.Item = this.Item.extend('Company', function() {
        this.addProperty('name', String);
        this.addIndex('name');
      });
    });

    repository = KindaLocalRepository.create(
      'Test',
      'mysql://test@localhost/test',
      [Elements, Accounts, People, Companies]
    );

    accounts = repository.createCollection('Accounts');
    accounts.context = {};

    people = repository.createCollection('People');
    people.context = {};

    companies = repository.createCollection('Companies');
    companies.context = {};
  });

  suiteTeardown(function *() {
    yield repository.destroyRepository();
  });

  test('get root collection class', function *() {
    var klass = repository.getRootCollectionClass();
    assert.strictEqual(klass.getName(), 'Elements');
  });

  test('repository id', function *() {
    var id = yield repository.getRepositoryId();
    assert.ok(id);
  });

  test('put, get and delete some items', function *() {
    var mvila = people.createItem({
      accountNumber: 12345,
      firstName: 'Manuel',
      lastName: 'Vila',
      country: 'France'
    });
    yield mvila.save();
    var id = mvila.id;
    var item = yield people.getItem(id);
    assert.strictEqual(item.accountNumber, 12345);
    assert.strictEqual(item.firstName, 'Manuel');
    yield item.delete();
    var item = yield people.getItem(id, { errorIfMissing: false });
    assert.isUndefined(item);
  });

  test('get a missing item', function *() {
    var err = yield catchError(function *() {
      yield people.getItem('xyz');
    });
    assert.instanceOf(err, Error);

    var item = yield people.getItem('xyz', { errorIfMissing: false });
    assert.isUndefined(item);
  });

  test('put an already existing item', function *() {
    var jack = people.createItem({ firstName: 'Jack', country: 'USA' });
    yield jack.save();
    var err = yield catchError(function *() {
      var jack2 = people.createItem({ id: jack.id, firstName: 'Jack', country: 'UK' });
      yield jack2.save();
    });
    assert.instanceOf(err, Error);

    yield jack.delete();
  });

  test('delete a missing item', function *() {
    var err = yield catchError(function *() {
      yield people.deleteItem('xyz');
    });
    assert.instanceOf(err, Error);

    var err = yield catchError(function *() {
      yield people.deleteItem('xyz', { errorIfMissing: false });
    });
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
      var items = yield accounts.getItems(['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].getClassName(), 'Account');
      assert.strictEqual(items[0].id, 'aaa');
      assert.strictEqual(items[0].accountNumber, 45329);
      assert.deepEqual(items[1].getClassName(), 'Company');
      assert.strictEqual(items[1].id, 'ccc');
      assert.strictEqual(items[1].accountNumber, 7002);
    });

    test('find all items in a collection', function *() {
      var items = yield companies.findItems();
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].getClassName(), 'Company');
      assert.strictEqual(items[0].id, 'ccc');
      assert.strictEqual(items[0].name, 'Kinda Ltd');
      assert.strictEqual(items[1].getClassName(), 'Company');
      assert.strictEqual(items[1].id, 'fff');
      assert.strictEqual(items[1].name, 'Fleur SARL');
    });

    test('find and order items', function *() {
      var items = yield people.findItems({ order: 'accountNumber' });
      assert.strictEqual(items.length, 3);
      var numbers = _.pluck(items, 'accountNumber');
      assert.deepEqual(numbers, [888, 3246, 55498]);
    });

    test('find items with a query', function *() {
      var items = yield accounts.findItems({ query: { country: 'USA' } });
      var ids = _.pluck(items, 'id');
      assert.deepEqual(ids, ['bbb', 'ddd']);

      var items = yield companies.findItems({ query: { country: 'UK' } });
      assert.strictEqual(items.length, 0);
    });

    test('count all items in a collection', function *() {
      var count = yield people.countItems();
      assert.strictEqual(count, 3);
    });

    test('count items with a query', function *() {
      var count = yield accounts.countItems({ query: { country: 'France' } });
      assert.strictEqual(count, 3);

      var count = yield people.countItems({ query: { country: 'France' } });
      assert.strictEqual(count, 1);

      var count = yield companies.countItems({ query: { country: 'Spain' } });
      assert.strictEqual(count, 0);
    });

    test('iterate over items', function *() {
      var ids = [];
      yield accounts.forEachItems({ batchSize: 2 }, function *(item) {
        ids.push(item.id);
      });
      assert.deepEqual(ids, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    test('find and delete items', function *() {
      var options = { query: { country: 'France' }, batchSize: 2 };
      yield accounts.findAndDeleteItems(options);
      var items = yield accounts.findItems();
      var ids = _.pluck(items, 'id');
      assert.deepEqual(ids, ['bbb', 'ccc', 'ddd']);
    });

    test('change an item inside a transaction', function *() {
      assert.isFalse(people.getRepository().isInsideTransaction());
      yield people.transaction(function *() {
        assert.isTrue(people.getRepository().isInsideTransaction());
        var item = yield people.getItem('bbb');
        assert.strictEqual(item.lastName, 'Daniel');
        item.lastName = 'D.';
        yield item.save();
        var item = yield people.getItem('bbb');
        assert.strictEqual(item.lastName, 'D.');
      });
      var item = yield people.getItem('bbb');
      assert.strictEqual(item.lastName, 'D.');
    });

    test('change an item inside an aborted transaction', function *() {
      var err = yield catchError(function *() {
        assert.isFalse(people.getRepository().isInsideTransaction());
        yield people.transaction(function *() {
          assert.isTrue(people.getRepository().isInsideTransaction());
          var item = yield people.getItem('bbb');
          assert.strictEqual(item.lastName, 'Daniel');
          item.lastName = 'D.';
          yield item.save();
          var item = yield people.getItem('bbb');
          assert.strictEqual(item.lastName, 'D.');
          throw new Error('something wrong');
        });
      });
      assert.instanceOf(err, Error);
      var item = yield people.getItem('bbb');
      assert.strictEqual(item.lastName, 'Daniel');
    });
  }); // 'with many items' suite
});
