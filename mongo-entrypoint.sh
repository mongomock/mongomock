#!/bin/bash
set -e

# Inicia mongod em background para inicialização
echo "Iniciando mongod para configuração..."
mongod --replSet rs0 --bind_ip_all --dbpath /data/db \
  --fork --logpath /var/log/mongod-init.log

# Aguarda mongod ficar pronto
until mongosh --eval "db.adminCommand('ping')" --quiet 2>/dev/null; do
  echo "Aguardando mongod..."
  sleep 2
done

# Verifica se replica set já está pronto
IS_READY=$(mongosh --eval "
  try { rs.status().ok === 1 && rs.isMaster().ismaster } catch(e) { false }
" --quiet 2>/dev/null)

if echo "$IS_READY" | grep -q true; then
  echo "Replica set já inicializado."
else
  # Inicializa replica set
  echo "Inicializando replica set..."
  mongosh --eval "
    try {
      rs.initiate({ _id: 'rs0', members: [{ _id: 0, host: 'mongo:27017' }] });
    } catch(e) {
      if (!e.message.includes('already initialized')) throw e;
    }
  " --quiet

  # Aguarda eleição do primário (timeout 30s)
  TIMEOUT=30
  ELAPSED=0
  until mongosh --eval "try{rs.isMaster().ismaster}catch(e){false}" \
      --quiet 2>/dev/null | grep -q true; do
    if [ $ELAPSED -ge $TIMEOUT ]; then
      echo "ERRO: Timeout aguardando eleição do primário (${TIMEOUT}s)"
      exit 1
    fi
    echo "Aguardando primário... (${ELAPSED}s/${TIMEOUT}s)"
    sleep 2
    ELAPSED=$((ELAPSED + 2))
  done
  echo "Primário eleito com sucesso."
fi

# Para mongod temporário
mongod --dbpath /data/db --shutdown 2>/dev/null || true

# Inicia mongod em foreground (processo principal do container)
echo "Iniciando mongod..."
exec mongod --replSet rs0 --bind_ip_all
