Aqui está o guia completo para configurar e rodar pela primeira vez:

## **1. Instalar pre-commit**
```bash
pip install pre-commit
# ou, se usar pipx:
pipx install pre-commit
```

## **2. Instalar os hooks no seu repositório**
Na raiz do projeto (mongomock), execute:
```bash
pre-commit install
```

Isso vai:
- Ler o arquivo .pre-commit-config.yaml
- Criar o arquivo `.git/hooks/pre-commit`
- Preparar o ambiente para rodar as verificações automaticamente antes de cada commit

## **3. Rodar a primeira vez manualmente (opcional, mas recomendado)**
```bash
# Verificar todos os arquivos do repositório
pre-commit run --all-files

# Ou apenas os arquivos staged (prontos para commit)
pre-commit run
```

## **O que o pre-commit vai verificar:**

- **ruff**: Linter que encontra erros e estilo de código
- **ruff-format**: Formata o código automaticamente
- **mypy**: Verifica tipos estáticos (type checking)

## **Workflow após configuração**

Toda vez que tentar fazer commit:
```bash
git add seus_arquivos.py
git commit -m "sua mensagem"
```

Se houver problemas:
- O pre-commit vai **parar o commit**
- Vai mostrar os erros encontrados
- Alguns (como formatação) são **corrigidos automaticamente**
- Outros você precisa **corrigir manualmente**

Depois é só fazer `git add` novamente e fazer commit de novo.

---

**Dica:** Se você quiser pular o pre-commit em um commit específico (não recomendado), use:
```bash
git commit --no-verify -m "sua mensagem"
```

Quer que eu execute os comandos de instalação para você?

## mypy config

```
# instalar dependências do projeto, incluindo extras pymongo
python -m pip install -e '.[pymongo]'
pre-commit run --all-files
pip install mypy
mypy --install-types
```

# para rodar toda a arvore de imports e pesquisar por mongomock (-p)
````
python -m mypy -p mongomock
```

## ruff


### Se NÃO estiver, instalar via pip
```bash
pip install ruff
```

### Para o projeto mongomock (com hatch)
```bash
# Se usando hatch
hatch env create

# Ou instale manualmente na env ativa
pip install ruff
```

---

## 2️⃣ Comandos Principais

### ✅ Ver erros de linting
```bash
ruff check mongomock/ tests/
```

### 🔧 Corrigir erros automaticamente
```bash
ruff check --fix mongomock/ tests/
```

### 🔧 Corrigir com "unsafe" fixes (mais agressivo)
```bash
ruff check --fix --unsafe-fixes mongomock/ tests/
```

### 📏 Formatar código (tipo black)
```bash
ruff format mongomock/ tests/
```

### 📋 Ver detalhes de um erro específico
```bash
ruff rule I001
ruff rule SIM108
```

---

## 3️⃣ Como Usar com Pre-commit (mongomock)

O projeto já usa **pre-commit hooks**. Para rodar:

### Rodar todos os hooks
```bash
pre-commit run --all-files
```

### Rodar apenas ruff
```bash
pre-commit run ruff --all-files
```

### Rodar apenas ruff-format
```bash
pre-commit run ruff-format --all-files