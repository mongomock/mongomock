Aqui está o guia completo para configurar e rodar pela primeira vez:

## **1. Instalar pre-commit**
```bash
pip install pre-commit
# ou, se usar pipx:
pipx install pre-commit
```

## **2. Instalar os hooks no seu repositório**
Na raiz do projeto (mongomock-ng), execute:
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
python -m mypy -p mongomock_ng
```

## ruff


### Se NÃO estiver, instalar via pip
```bash
pip install ruff
```

### Para o projeto mongomock-ng (com hatch)
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
ruff check mongomock_ng/ tests/
```

### 🔧 Corrigir erros automaticamente
```bash
ruff check --fix mongomock_ng/ tests/
```

### 🔧 Corrigir com "unsafe" fixes (mais agressivo)
```bash
ruff check --fix --unsafe-fixes mongomock_ng/ tests/
```

### 📏 Formatar código (tipo black)
```bash
ruff format mongomock_ng/ tests/
```

### 📋 Ver detalhes de um erro específico
```bash
ruff rule I001
ruff rule SIM108
```

---

## 3️⃣ Como Usar com Pre-commit (mongomock-ng)

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

### LLM otimizations:

Entendo que queira usar o RTK e o Caveman para diminuir o consumo de tokens no seu projeto. Como vimos, isso é uma ótima saída para contornar os limites de uso e manter o ritmo de desenvolvimento.

Vou explicar de forma prática como integrar essas duas ferramentas no seu fluxo com o GitHub Copilot. A ideia delas é complementar: enquanto o **RTK filtra e comprime o que o seu agente *vê*** (os comandos do terminal, como `git diff`), o **Caveman otimiza o que ele *fala***, forçando respostas super diretas e sem enrolação. Juntos, podem gerar uma economia bem grande de tokens.

### ⚙️ Passo 1: Instalando o RTK (Rust Token Killer)

O RTK age como um "filtro" silencioso. Ele intercepta os comandos rodados pelo agente e remove toda a "gordura" (mensagens de ajuda, cabeçalhos, informações repetidas) do retorno, entregando só o que realmente importa para o contexto.

*   **Instalação**: Abra o terminal e rode um dos comandos abaixo:
    *   **macOS (Homebrew)**: ```bash
        brew install rtk
        ```
    *   **Linux / WSL**: ```bash
        curl -fsSL https://raw.githubusercontent.com/rtk-ai/rtk/main/install.sh | bash
        ```
    *   **Alternativa (Cargo)**: Se você já tem o Rust instalado, use: ```bash
        cargo install --git https://github.com/rtk-ai/rtk
        ```
*   **Ativação para o GitHub Copilot**: Para que o VS Code use o RTK, é necessário configurar um Hook. A maneira mais comum é usar o `rtk init` no diretório do seu projeto ou globalmente.
    ```bash
    rtk init
    ```
    Este comando deve configurar os arquivos necessários para que os comandos do terminal, quando chamados pelo Copilot, sejam roteados através do filtro do RTK automaticamente.

### 🗿 Passo 2: Instalando e Usando o Caveman

O Caveman é uma "skill" (conjunto de instruções) que ensina o seu Agente a ser extremamente conciso, cortando cerca de 75% do texto das respostas sem perder a precisão técnica.

*   **Instalação**: No terminal, execute o comando de instalação universal. O script detectará automaticamente as ferramentas disponíveis no seu ambiente.
    *   **macOS / Linux / WSL**:
        ```bash
        curl -fsSL https://raw.githubusercontent.com/JuliusBrussee/caveman/main/install.sh | bash
        ```
    *   **Windows (PowerShell)**:
        ```powershell
        irm https://raw.githubusercontent.com/JuliusBrussee/caveman/main/install.ps1 | iex
        ```
    A instalação leva cerca de 30 segundos e requer Node.js versão 18 ou superior.
*   **Como Usar no Chat do Copilot**: Dentro do chat do GitHub Copilot no VS Code, digite `/caveman` e escolha o nível de concisão que deseja:
    *   `lite`: Remove apenas palavras de preenchimento.
    *   `full`: Modo padrão "caveman", bem direto.
    *   `ultra`: Extremamente telegráfico.
    *   `wenyan`: Compressão máxima, inspirada no chinês clássico.

    > ⚠️ **Importante**: O Caveman é ativado manualmente no chat. Cada nova conversa pode precisar do comando `/caveman` novamente.

### 📊 Quanto Vamos Economizar?

Aqui está uma comparação do que você pode esperar de economia com cada ferramenta:

| Cenário | Sem Otimização (Tokens) | Com Caveman (Output) | Com RTK (Input) |
| :--- | :--- | :--- | :--- |
| **Resposta do Chat** | Resposta normal e detalhada (ex: 69 tokens) | Resposta curta e direta (ex: ~19 tokens) | - |
| **Comando `git diff`** | Cabeçalhos + mudanças (ex: ~12.000 tokens) | - | Apenas as mudanças (ex: ~960 tokens) |
| **Refatoração (12 arquivos)** | Histórico + logs de sucesso (ex: ~74.700 tokens) | - | Agrupamento de informações (ex: ~6.960 tokens) |
| **Sessão de 30 minutos** | Relatório completo e verboso (ex: ~118.000 tokens) | - | Foco em falhas e resumos (ex: ~23.900 tokens) |

### 💎 Resumo e Próximos Passos

Com o RTK cuidando para que o Copilot gaste menos tokens processando o que ele *vê* (inputs), e o Caveman reduzindo o que ele *fala* (outputs), você cria um ambiente muito mais econômico.

O RTK (filtro de comandos) é um ótimo começo. Se você notar que as respostas do seu agente ainda estão muito longas, ativar o modo "caveman" no chat será o passo seguinte para maximizar a economia. Depois de instalar, me diga se rodou tudo certo ou se teve alguma dificuldade na configuração que possamos resolver juntos.