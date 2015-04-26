var repository = LocalRepository.create('Test', 'mysql://...', [
  People,
  Images,
  Songs
]);

var peopleCollection = repository.createCollection('People');
