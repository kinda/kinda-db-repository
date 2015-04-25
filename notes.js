var repository = LocalRepository.create('Test', db, [
  People,
  Images,
  Songs
]);

var peopleCollection = repository.createCollection('People');
