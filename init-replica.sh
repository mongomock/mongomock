#!/bin/bash
set -e

# Wait for MongoDB to be ready
until mongosh --eval "db.adminCommand('ping')" --quiet 2>/dev/null; do
  echo "Waiting for MongoDB to be ready..."
  sleep 1
done

# Initialize replica set if not already initialized
mongosh --eval "
try {
  rs.status();
  print('Replica set already initialized');
} catch(e) {
  print('Initializing replica set...');
  rs.initiate({
    _id: 'rs0',
    members: [{ _id: 0, host: 'mongo:27017' }]
  });
  // Wait for replica set to be ready
  while (!rs.isMaster().ismaster) {
    sleep(1000);
  }
  print('Replica set initialized and ready');
}
"
