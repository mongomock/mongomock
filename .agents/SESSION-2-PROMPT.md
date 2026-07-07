## 🎯 SESSION 2+ PROMPT

Use este prompt na próxima sessão para retomar o trabalho:

---

### RESUMO EXECUTIVO

**Projeto:** mongomock-ng geospatial operators PR
**Branch:** `feat/geospatial-support`
**Status:** 91 testes, 89% coverage geospatial.py, 4 bugs críticos já corrigidos
**Atividade:** Code review, testes, bug fixes
**Próximo passo:** 3 bugs restantes + melhorar coverage

### CONTEXT RÁPIDO

Session 1 completou:
- ✅ Code review completo (12 findings)
- ✅ 91 testes (+29 novos, todos passando)
- ✅ Coverage 62%→89% em geospatial.py
- ✅ 4 bugs críticos corrigidos via cavecrew-builder

### TODO IMEDIATO (Session 2)

**ALTA PRIORIDADE:**
1. [ ] geospatial.py:L246-263 — Extend geo_intersects/geo_within para LineString/Polygon docs
   - Use cavecrew-builder
   - Test: `pytest tests/test__geospatial.py::GeoIntersectsTest -v`

2. [ ] geospatial.py:L129 — Float comparison epsilon na validação de polygon ring
   - Trocar: `ring[0] != ring[-1]` 
   - Por: `not _points_equal(ring[0], ring[-1])`
   - Use cavecrew-builder

3. [ ] collection.py:L1433 — Clarify distance aggregation (min() on multiple near fields)
   - Adicionar comentário explicativo
   - Considerar teste para múltiplos near fields
   - Use cavecrew-builder

**MÉDIA PRIORIDADE:**
4. [ ] Adicionar 10-15 testes para aggregate.py $geoNear (target: 25% coverage)
5. [ ] Adicionar 5-10 testes para collection.py near sorting (target: 30% coverage)

**FINALIZAÇÃO:**
6. [ ] Rodar full test suite — verificar regressions
7. [ ] caveman-commit para gerar mensagem
8. [ ] git commit
9. [ ] Pronto para PR

### COMANDO QUICK START

```bash
cd /Users/felipemonteirojacome/workspace/mongomock-ng

# Verificar status
git status
.env/bin/python -m pytest tests/test__geospatial.py -v

# Ver contexto completo
cat .agents/session-context.md

# Coverage report
.env/bin/python -m pytest tests/test__geospatial.py \
  --cov=mongomock_ng.geospatial \
  --cov=mongomock_ng.aggregate \
  --cov=mongomock_ng.filtering \
  --cov=mongomock_ng.collection \
  --cov-report=term-missing
```

### FILES PRINCIPAIS

- `mongomock_ng/geospatial.py` — Main implementation (435 lines, 89% covered)
- `mongomock_ng/filtering.py` — Query operators (45% covered)
- `mongomock_ng/aggregate.py` — Pipeline stages (13% covered)
- `mongomock_ng/collection.py` — Collection find() (22% covered)
- `tests/test__geospatial.py` — Test suite (91 tests, all passing)

### FERRAMENTAS USAR

```
cavecrew-builder  — para fixes cirúrgicos (1-2 files)
caveman-commit    — para commit message
caveman-review    — para final review antes de commit
```

### MÉTRICAS ATUAIS

```
Tests:       91/91 passing ✅
Coverage:    geospatial.py 89%
Files:       4 modified
Lines:       +625, -7
Bugs fixed:  4 critical
```

---

**COMEÇAR POR:** Ler `.agents/session-context.md` para detalhes completos, depois rodar pytest para verificar status.
