"use strict";

require('co-mocha');
var assert = require('chai').assert;
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

  test('simple putItem, getItem and deleteItem', function *() {
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
    });

    teardown(function *() {
      yield users.deleteItem('aaa');
      yield users.deleteItem('bbb');
      yield users.deleteItem('ccc');
    });

    test('getItems', function *() {
      var items = yield users.getItems(['aaa', 'ccc']);
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].firstName, 'Bob');
      assert.strictEqual(items[1].firstName, 'Alan');
    });

    test('findItems', function *() {
      var items = yield users.findItems({ start: 'bbb', end: 'ccc' });
      assert.strictEqual(items.length, 2);
      assert.strictEqual(items[0].firstName, 'Jack');
      assert.strictEqual(items[1].firstName, 'Alan');
    });

    test('countItems', function *() {
      var count = yield users.countItems();
      assert.strictEqual(count, 3);
      var count = yield users.countItems({ start: 'bbb', end: 'ccc' });
      assert.strictEqual(count, 2);
    });
  });
});
