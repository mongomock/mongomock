# Arch
os testes utilizam um servidor mongodb que acessa através da variavel  de ambiente(TEST_MONGO_HOST).

afim de facilitar o desenvolvimento:


# podman
podman pull docker.io/library/mongo:5.0.5

rsync:
rsync -avz -e "ssh -i ~/.ssh/id_ed25519" ./ felipe@10.119.88.43:~/workspace/mongomock

rsync -avz -e "ssh -i ~/.ssh/id_ed25519" ~/workspace/mongomock/* felipe@10.119.88.43:~/workspace/mongomock

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
================ 866 passed, 25 skipped, 241 warnings in 3.81s =================



ssh felipe@10.119.88.43:~/workspace/mongomock
