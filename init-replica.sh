#!/bin/bash
set -e

# Aguarda o MongoDB ficar disponível
until mongosh --host mongo --eval "db.adminCommand('ping')" --quiet 2>/dev/null; do
  echo "Aguardando MongoDB..."
  sleep 2
done

# Inicializa o replica set se ainda não existir
mongosh --host mongo --eval "
rs.status().ok === 1 ?
  print('Replica set já inicializado') :
  (print('Inicializando replica set...'), 
   rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'mongo:27017' }] }),
   sleep(2000))"

# Aguarda até que o nó atual seja primário
until mongosh --host mongo --eval "rs.isMaster().ismaster" --quiet | grep -q true; do
  echo "Aguardando eleição do primário..."
  sleep 2
done

echo "Replica set pronto para uso."