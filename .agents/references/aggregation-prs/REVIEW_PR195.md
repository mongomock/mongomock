# Code Review — PR #195 (pr/aggregation-consolidated)

Use `cavecrew-reviewer` ou vanilla `Code Reviewer` para revisar o PR `pr/aggregation-consolidated` → `develop`. Commit: `3d2342b`.

## Escopo

Consolida **10 PRs de aggregation** do mongomock original em mongomock-ng.

| Arquivo | Inserções | Deleções |
|---|---|---|
| `mongomock_ng/aggregate.py` | ~900 | ~120 |
| `tests/test__collection_api.py` | ~500 | ~0 |
| `tests/test__mongomock.py` | ~400 | ~0 |
| `CHANGELOG.md` | 12 | 0 |

---

## aggregate.py — Operadores

### 1. `$round` (PR #930)

```
mongomock_ng/aggregate.py:93-102  — unary_arithmetic_operators (NÃO inclui $round)
mongomock_ng/aggregate.py:103    — binary_arithmetic_operators_with_optional_second_number = {'$round'}
mongomock_ng/aggregate.py:104-112 — binary_arithmetic_operators (inclui $round via união)
mongomock_ng/aggregate.py:427-440 — handler: 1 ou 2 params, round(number_0, number_1)
```

- `$round` está APENAS em binary, NÃO em unary. Correto?
- Suporta `{$round: 9.51}` (1 param) e `{$round: [9.51, 1]}` (2 params). Correto?
- `round(number_0, number_1)` — quando `number_1` é `None` (1 param), `round(x, None)` lança TypeError. Isso é o que MongoDB faz?

### 2. `$reduce` (PR #820)

```
mongomock_ng/aggregate.py:262-264  — parse(): list → self.parse_many()
mongomock_ng/aggregate.py:804-828  — handler em _handle_array_operator
```

- `parse()` trata `isinstance(expression, list)` → pode conflitar com operadores que recebem list como argumento (ex: `$add`, `$multiply`)? Esses são pegos ANTES no dispatch.
- `$$this` e `$$value` via `dict(self._user_vars, this=item, value=current)`. Correto?
- `_Parser` criado para cada iteração — performance aceitável?

### 3. `$type` (PR #929)

```
mongomock_ng/aggregate.py:188-191  — type_operators inclui '$type'
mongomock_ng/aggregate.py:1134-1159 — handler em _handle_type_operator
```

- bool → "bool". str → "string". dict → "object". list/tuple → "array". None → "null". int > 2³¹-1 → "long". int → "int". datetime → "date". KeyError → "missing". Correto?
- `return 'missing'` dentro do `except KeyError` — se `parsed` é resolvido mas é outro tipo não mapeado, cai no `raise NotImplementedError`. Correto?
- float → cai no `raise NotImplementedError`. MongoDB retorna "double". Deveria ser adicionado?

### 4. `$convert` (PR #864) — Refactor

```
mongomock_ng/aggregate.py:909     — _handle_type_convertion_operator → dispatch
mongomock_ng/aggregate.py:913-987  — handlers separados (toString, toInt, toLong, toDecimal, arrayToObject, objectToArray)
mongomock_ng/aggregate.py:989-1019 — _handle_type_convertion_to_object_id
mongomock_ng/aggregate.py:1021-1048 — _handle_convert ($convert)
mongomock_ng/aggregate.py:1050-1052 — _raise_convert_not_implemented (factory)
mongomock_ng/aggregate.py:1054-1074 — _TYPE_CONVERTION_HANDLERS (ClassVar)
mongomock_ng/aggregate.py:1076-1093 — _CONVERT_TO_HANDLERS (ClassVar)
```

- `_TYPE_CONVERTION_HANDLERS` mapeia `$toString`→`_handle_type_convertion_to_string`, etc. Inclui `$toObjectId` e `$convert`. Correto?
- `_CONVERT_TO_HANDLERS` mapeia 'string'→toString, 'int'→toInt, 'long'→toLong, 'decimal'→toDecimal, 2→toString, 16→toInt, 18→toLong, 19→toDecimal. Outros → NotImplementedError. Correto?
- `_handle_convert`: se `onError` ou `onNull` presentes → NotImplementedError. Depois busca handler em `_CONVERT_TO_HANDLERS[to_]`. Correto?
- `_TYPE_CONVERTION_HANDLERS` e `_CONVERT_TO_HANDLERS` anotados como `ClassVar`. Correto?

### 5. `$toObjectId` (PR #935)

```
mongomock_ng/aggregate.py:989-1019 — _handle_type_convertion_to_object_id
```

- Trata ObjectId (retorna), str (parseia), None (retorna). KeyError → None. Outro tipo → OperationFailure. Correto?
- Captura `ValueError`, `TypeError`, `InvalidId` e converte para `OperationFailure`. `InvalidId` importado de `bson.errors` no bloco try/except. Correto?
- A indentação: `if isinstance(parsed, str)` NÃO está dentro do bloco `if isinstance(parsed, helpers.ObjectId)`. Verificar.

### 6. `$sortByCount` (PR #896)

```
mongomock_ng/aggregate.py:1676-1690 — _handle_sort_by_count_stage
```

- Dict options → NotImplementedError. String → field name. Counter + most_common(). Correto?
- Ordenação: count desc + primeira ocorrência (most_common()). MongoDB ordena count desc + _id asc. Diferença aceitável?
- Testes usam `self.cmp.compare` (não ignore_order) e inserem dados específicos. Passa?

### 7. `$fill` (PR #892)

```
mongomock_ng/aggregate.py:1698-1708 — _handle_fill
```

- Implementação simplificada: só pega primeira chave de `options['output']` e seu `value`. Não suporta `method`, `sortBy`, `partitionByFields`, `partitionBy`. Correto para feature incompleta?
- Se `key_to_fill` não existe no doc, adiciona. Se existe, mantém original. Correto?

### 8. `$unset` (PR #925)

```
mongomock_ng/aggregate.py:1700-1736 — _handle_unset_stage
```

- Aceita string (field único) ou list (múltiplos). Outro tipo → OperationFailure. Correto?
- Dot-notation: `field.split('.')`, percorre `parts[:-1]`, se sub_doc não é dict → break (for/else). Correto?
- `sub_doc.pop(parts[-1], None)` — remove sem erro se não existe. Correto?
- `$unset` já estava em `VALID_UPDATE_PIPELINE_STAGES` em collection.py. OK.

### 9. `$setWindowFields` (PR #821)

```
mongomock_ng/aggregate.py:54-79    — set_window_fields_operators list
mongomock_ng/aggregate.py:1400-1440 — _accumulate_set_window_fields
mongomock_ng/aggregate.py:1630-1662 — _handle_set_window_fields_stage
mongomock_ng/aggregate.py:1962    — registro em _PIPELINE_HANDLERS
```

- `set_window_fields_operators` lista todos os operadores suportados. Só `$shift` implementado. Correto?
- `_accumulate_set_window_fields`: valida operador, `window` field → NotImplementedError, `$shift` → implementado. Correto?
- `$shift`: requer `sortBy`, lê `output`, `by`, `default`. Shift positivo = shift para frente. Correto?
- `_handle_set_window_fields_stage`: partitionBy via groupby, sortBy via _handle_sort_stage. Correto?

### 10. Timezone expression (PR #822)

```
mongomock_ng/aggregate.py:665-666 — _handle_date_operator
```

- Antes: `target_tz = pytz.timezone(values['timezone'])`
- Depois: `tz = self.parse(values['timezone'])` + `target_tz = pytz.timezone(tz)`
- Permite `$info.tmz` como expressão. Correto?

---

## Testes

### test__collection_api.py

| Teste | O que cobre |
|---|---|
| `test__aggregate_to_object_id` | string→OID, OID→OID, null→null, missing→null, string inválida→erro, tipo inválido→erro |
| `test__aggregate_fill` | campo ausente preenchido com valor default |
| `test_aggregate_type` | bool→"bool", str→"string", int→"int"/"long", list→"array", None→"null", datetime→"date", missing→"missing" |
| `test__aggregate_reduce` | soma, $val como initialValue, empty list, None input, nested reduce, missing key, erros de validação (4 casos) |
| `test__aggregate_set_window_fields_basic` | sem output, operador inválido, $sum (NotImplemented), $shift com window (NotImplemented) |
| `test__aggregate_set_window_fields_shift` | partitionBy + sortBy + $shift funcional, sem sortBy (erro) |
| `test__update_pipeline` | casos $unset string e list com dot-notation |

### test__mongomock.py

| Teste | O que cobre |
|---|---|
| `test_aggregate_date_with_timezone_expression` | `$info.tmz` como expressão em $year/$week/$dayOfWeek |
| `test__aggregate20` | `$round: [9.51, 1]` adicionado no projection |
| `test__aggregate_exception` | `$round: '12'` → OperationFailure |
| `test__aggregate_type` | tipos + `missing` |
| `test__aggregate_reduce` | comparação mock vs real (reduce + erros) |
| `test_aggregate_convert` | `$convert` com to: string/int/long/decimal/2/16/18/19 |
| `test__aggregate_sort_by_count` | `$facet` + `$sortByCount` (com dados específicos) |
| `test__set_window_fields_*` | 4 testes: no output, $shift funcional, partition none, operador inválido |

---

## Critérios

1. 🟥 **Fidelidade MongoDB**: quando mock e real divergem, prefira o comportamento do real (tipo da exceção, mensagem, momento). Se pymongo lança `OperationFailure`, mock deve lançar `OperationFailure`.
2. 🟥 **Safety**: nenhum operador existente quebra. `$round` NÃO entra no fluxo unary. `parse()` list handling não conflita com operadores existentes.
3. 🟥 **Edge cases**: None, KeyError (missing field), tipos inesperados, listas vazias.
4. 🟡 **Type hints**: ClassVar, tipos de retorno, parâmetros de callback.
5. 🟡 **Cobertura**: todo código novo tem teste correspondente?
6. 🔵 **Estilo**: single quotes, line length 100, imports organizados.

---

## Output

```
path:linha: 🟥/🟡/🔵/❓ severidade: problema. sugestão.
```

Classificar:
- 🟥 crítico — quebra funcionalidade ou divergência grave do MongoDB
- 🟡 médio — problema menor, edge case não tratado, type hint faltando
- 🔵 informativo — sugestão de melhoria, estilo, refatoração
- ❓ dúvida — comportamento incerto, precisa verificar
