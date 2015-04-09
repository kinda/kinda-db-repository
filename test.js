"use strict";

require('co-mocha');
var assert = require('chai').assert;
var _ = require('lodash');
var Collection = require('kinda-collection');
var KindaDB = require('kinda-db');
var KindaDBRepository = require('./');

suite('KindaDBRepository', function() {
  var users;

  suiteSetup(function *() {
    var db = KindaDB.create('Test', 'mysql://test@localhost/test');
    db.registerMigration(1, function *() {
      yield this.addTable('Users');
    });
    yield db.initializeDatabase();

    var repository = KindaDBRepository.create(db);

    var Users = Collection.extend('Users', function() {
      this.Item = this.Item.extend('Checking', function() {
        this.addPrimaryKeyProperty('id', String);
        this.addProperty('firstName', String);
        this.addProperty('age', Number);
      });

      this.setRepository(repository);
    });

    users = Users.create();
  });

  suiteTeardown(function *() {
    yield users.getRepository().getDatabase().destroyDatabase();
  });

  test('put, get and delete some items', function *() {
    var mvila = users.createItem({ firstName: 'Manu', age: 42 });
    yield mvila.save();
    var id = mvila.id;
    var item = yield users.getItem(id);
    assert.strictEqual(item.firstName, 'Manu');
    assert.strictEqual(item.age, 42);
    yield item.delete();
    var item = yield users.getItem(id, { errorIfMissing: false });
    assert.isUndefined(item);
  });

  suite('with many items', function() {
    setup(function *() {
      yield users.putItem({ id: 'aaa', firstName: 'Bob' });
      yield users.putItem({ id: 'bbb', firstName: 'Jack' });
      yield users.putItem({ id: 'ccc', firstName: 'Alan' });
      yield users.putItem({ id: 'ddd', firstName: 'Joe' });
      yield users.putItem({ id: 'eee', firstName: 'John' });
    });

    teardown(function *() {
      yield users.deleteItem('aaa', { errorIfMissing: false });
      yield users.deleteItem('bbb', { errorIfMissing: false });
      yield users.deleteItem('ccc', { errorIfMissing: false });
      yield users.deleteItem('ddd', { errorIfMissing: false });
      yield users.deleteItem('eee', { errorIfMissing: false });
    });

    test('get many items by id', function *() {
      var items = yield users.getItems(['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].firstName, 'Bob');
      assert.strictEqual(items[1].firstName, 'Alan');
    });

    test('find items between two existing items', function *() {
      var items = yield users.findItems({ start: 'bbb', end: 'ccc' });
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].firstName, 'Jack');
      assert.strictEqual(items[1].firstName, 'Alan');
    });

    test('count items', function *() {
      var count = yield users.countItems();
      assert.strictEqual(count, 5);
      var count = yield users.countItems({ start: 'bbb', end: 'ccc' });
      assert.strictEqual(count, 2);
    });

    test('iterate over items', function *() {
      var ids = [];
      var options = { start: 'bbb', end: 'ddd', batchSize: 2 };
      yield users.forEachItems(options, function *(item) {
        ids.push(item.id);
      });
      assert.deepEqual(ids, ['bbb', 'ccc', 'ddd']);
    });

    test('find and delete items', function *() {
      yield users.findAndDeleteItems({ start: 'bbb', end: 'ddd', batchSize: 2 });
      var items = yield users.findItems();
      var ids = _.pluck(items, 'id');
      assert.deepEqual(ids, ['aaa', 'eee']);
    });
  });
});
