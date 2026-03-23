# Controle Financeiro

Ferramenta financeira completa e leve construída com Python + Flask + SQLite. **Etapa 18 implementada.**

## Stack
- Python 3.11
- Flask
- SQLite3 (nativo do Python)
- HTML + CSS + JS puro (sem frameworks externos)

## Estrutura
```
app.py              # Aplicação principal (rotas, DB, lógica)
database.db         # Banco SQLite gerado automaticamente
templates/
  index.html               # Dashboard principal
  gestao.html              # Gestão: clientes, serviços, faturamentos
  contador.html            # Simulador fiscal (PF/MEI/PJ)
  notas.html               # Notas Fiscais — lista e formulário
  recorrentes.html         # Lançamentos Recorrentes (etapa 13)
  configuracao_fiscal.html # Configuração fiscal do emissor
  inteligencia.html        # Motor de Inteligência Financeira (score, análise, alertas, sugestões)
  integracao_fiscal.html   # Configuração da integração NFS-e
  status_fiscal.html       # Painel de status fiscal de todas as notas
  ver_nota.html            # Visualização de nota + seção de integração
  nota_eventos.html        # Histórico de eventos fiscais de uma nota
  editar_nota.html         # Edição de nota (apenas rascunhos)
  editar_meta.html         # Edição de meta financeira
  editar_cliente.html      # Edição de cliente
  editar_servico.html      # Edição de serviço
static/
  style.css           # Estilos responsivos sem bibliotecas externas
  app.js              # JS compartilhado: sidebar toggle, modal, toast
```

## Módulos implementados

### Etapas anteriores
- Dashboard financeiro com lançamentos (entradas/despesas)
- Filtros por período (hoje, 7d, 30d, mês, todos)
- Alertas e resumo inteligente
- Metas financeiras (meta, reserva, investimento)
- Patrimônio e reserva de emergência
- Simulador fiscal (PF / MEI / PJ – Simples Nacional)
- Gestão de clientes, serviços e faturamentos
- Marcação de pagamento com lançamento automático

### Etapa 7 — Notas Fiscais Internas
- Seção dedicada em /notas com resumo fiscal (cards)
- Numeração automática: NF-000001, NF-000002 ...
- Criação manual com todos os campos
- Geração automática a partir de faturamentos pagos
- Preenchimento automático de dados do faturamento
- Status: rascunho / emitida / cancelada
- Histórico com filtros visuais de status
- Ações inline na lista: Ver, Emitir, Editar, Cancelar, Excluir
- Página de visualização organizada da nota
- Botão de impressão (CSS @media print)
- Faturamento vinculado aparece na nota
- Prevenção de duplicidade: mesmo faturamento não gera 2 notas
- Botão "Gerar NF" / "Ver NF" na lista de faturamentos pagos

## Banco de Dados (SQLite)
Tabelas:
- `lancamentos` — entradas e despesas
- `metas` — metas financeiras, reservas, investimentos
- `perfil_fiscal` — perfil para simulação fiscal
- `clientes` — cadastro de clientes (PF/PJ)
- `servicos` — catálogo de serviços
- `faturamentos` — faturamentos por cliente/serviço
- `notas_fiscais` — notas fiscais internas + colunas `status_integracao`, `payload_fiscal`
- `configuracao_fiscal` — dados do emissor (razão social, CNPJ, endereço, regime)
- `integracao_fiscal_config` — configuração de integração (modo, provedor, ambiente, município)
- `eventos_fiscais` — histórico de eventos de integração por nota (preparação, simulação, etc.)

## Etapa 10 — Dashboard Melhorado + Configuração Fiscal
- Barra de atalhos rápidos no topo do dashboard (Entrada, Despesa, Cliente, Serviço, Faturamento, Nota, Meta, Backup)
- 3 novos cards no dashboard: Faturamento do Mês, Notas Emitidas (mês), Metas em Andamento
- Subtítulo descritivo em todos os cards
- Seção "Últimas Movimentações": 5 últimos lançamentos + 3 faturamentos + 3 notas
- Seção "Pendências": faturamentos pendentes, notas em rascunho, metas com prazo vencido, alertas
- Seção "Resumo Fiscal" no dashboard com status da integração e link para configuração
- Nova página /configuracao-fiscal: formulário completo de dados do emissor (razão social, CPF/CNPJ, endereços, regime tributário etc.)
- Painel "Status da Integração Fiscal" com indicadores claros do estado atual do sistema
- Aviso transparente sobre emissão interna vs. integração oficial futura

## Etapa 12 — Motor de Inteligência Financeira
- Novo módulo `/inteligencia` com 6 seções interligadas
- **Score Financeiro (0–100)**: calculado com lógica de saldo, taxa de poupança, ratio de despesas e meses negativos; mostra barra visual + fatores explicativos individuais
- **Resumo Executivo automático**: 4 blocos (situação atual, principal risco, melhor oportunidade, próxima ação), gerados dinamicamente com base nos dados reais
- **Análise de Comportamento**: médias de entradas/saídas/saldo, taxa de poupança, comparação mês atual vs anterior, categoria que cresceu acima do normal, barras visuais das maiores categorias de gasto
- **Alerta Inteligente**: alertas contextuais com prioridade (alto/médio/baixo) — variação de gastos, queda de renda, saldo negativo, despesas fixas altas, etc.
- **Detector de Erros Financeiros**: identifica padrões problemáticos com título + explicação + impacto + sugestão de correção (por nível de risco)
- **Sugestões Inteligentes**: geradas com valores reais do sistema (ex: "reduza 15% em Moradia e libere R$ 900")
- Estado "sem dados": tela amigável com link para registrar lançamentos
- Funções auxiliares puras: `_buscar_dados_mensais`, `_calcular_score`, `_detectar_erros`, `_gerar_alertas`, `_gerar_sugestoes`, `_gerar_resumo`, `calcular_inteligencia_financeira`, `faixa_score`
- Sem dependências externas — lógica baseada em médias, percentuais e comparações simples
- CSS completo: grids responsivos, cards coloridos por contexto, barras CSS, badges de prioridade
- Nav "Inteligência" adicionado nos templates principais

## Etapa 11 — Infraestrutura de Integração NFS-e
- Tabelas `integracao_fiscal_config` e `eventos_fiscais` criadas
- Colunas `status_integracao` e `payload_fiscal` adicionadas a `notas_fiscais`
- Funções auxiliares: `validar_config_fiscal`, `montar_payload_fiscal`, `registrar_evento_fiscal`
- Dicionário `LABEL_INTEGRACAO` com mapeamento de status → (ícone, label, classe CSS)
- 8 valores de `status_integracao`: nao_enviada, pronta_para_envio, enviada_homologacao, enviada_producao, autorizada, rejeitada, cancelamento_pendente, cancelada_externamente
- Nova página `/integracao-fiscal`: configurar modo de emissão, provedor, ambiente, município
- Nova página `/status-fiscal`: tabela de todas as notas com status de integração + contadores
- Nova página `/notas/eventos/<id>`: histórico de eventos fiscais de cada nota
- Rota `POST /notas/preparar/<id>`: valida dados, gera payload JSON, marca como pronta_para_envio
- Rota `POST /notas/simular/<id>`: simula envio com protocolo fictício (SIM-XXXX), sem API real
- `ver_nota.html` atualizado: seção de integração com status badge, botões Preparar + Simular + Eventos
- Nav atualizado nos templates principais: links para Fiscal, Integração, Status NF
- CSS completo para todos os novos componentes (badges, contadores, evento cards, payload viewer)

## Etapa 16 — Motor de Padrões + Previsões + Alertas Preventivos + Automações + Plano de Ação
- **Motor de Padrões** (`_detectar_padroes`): analisa histórico de lançamentos, agrupa por descrição, calcula valor médio/desvio, dia médio/desvio, frequência e meses distintos; classifica confiança (alta/média/baixa); armazena em `padroes_aprendidos`
- **Previsões de Lançamentos** (`_gerar_previsoes`): identifica padrões não lançados no mês atual e gera previsões com data, valor e categoria estimados; formulário inline para registrar 1-clique
- **Alertas Preventivos** (`_alertas_preventivos`): projeta saldo ao fim do mês com base no ritmo diário de gastos; alerta se projeção for negativa ou abaixo de 8% da renda; compara com mês anterior
- **Automações Inteligentes** (`_gerar_automacoes`): sugere criar recorrente para padrão forte; registrar previsão pendente; avançar meta com saldo disponível; gerar nota fiscal pendente; vincular faturamento pago ao controle financeiro
- **Plano de Ação** (`_gerar_plano_acao`): consolida alertas críticos, previsões de alta confiança, erros e automações em até 5 passos ordenados por prioridade
- **Rota `/previsao/registrar`** (`POST`): registra previsão como lançamento real, previne duplicatas, incrementa frequência no padrão aprendido
- **Tabela `padroes_aprendidos`** adicionada ao `init_db()`: persiste padrões detectados com chave, categoria, valor_medio, frequência, confiança e última ocorrência
- **`sugerir_categoria()` refatorada**: retorna `dict {categoria, subtipo, valor, confianca}` em vez de tupla
- **CSS completo**: `plano-*`, `automacao-*`, `padrao-*`, `previsao-*` — grades responsivas, badges de confiança, botões de ação inline

## Etapa 15 — Motor de Inteligência Avançado + Importação CSV
- **Importação CSV** (`/importar`): upload com drag & drop, prévia editável linha a linha, detecção automática de colunas (data/valor/tipo/categoria/subtipo), sugestão automática de categoria, proteção contra duplicatas (data + valor + descrição), resumo da importação
- **Comparação histórica**: mês atual vs. mês anterior e vs. média histórica, com variação percentual por categoria; bloco visual `comp-hist-grid`
- **Análise de tendência** (`tend-grid`): tendência de renda, gastos e saldo nos últimos 3 meses com mini barras CSS; categorias em alta contínua; meses negativos consecutivos
- **Alertas personalizados históricos**: gastos acima do padrão, queda de renda vs. mês anterior, meses seguidos negativos, categoria crescendo por 3 meses, taxa de poupança, renda abaixo da média
- **Perfil Financeiro** (`perfil-label-badge`): renda média, gastos médios, capacidade de guardar, % de despesas fixas/variáveis, top categorias com barras CSS; perfil: Poupador/Boa Gestão/Atenção/Risco
- **Score refinado**: fator de tendência (saldo crescendo mês a mês), consistência de saldos positivos, histórico de meses negativos; fatores explicativos com pontuação individual
- **Resumo executivo avançado**: inclui contexto de tendência (melhorando, caindo, instável), detecta renda instável e padrão de gasto piorando
- **CSS completo adicionado**: `comp-hist-*`, `comp-var-*`, `comp-cat-*`, `tend-*`, `perfil-*`, `badge-nivel-*`, `import-*`, `btn-importar-submit`, `dropzone-over` — todas responsivas

## Etapa 14 — UX Major Polish: Sidebar Navigation
- Sidebar fixo de 220px substituiu a barra de navegação superior em todos os 18 templates
- Seções organizadas: Principal, Financeiro, Ferramentas, Sistema
- Link ativo destacado com borda azul esquerda (`.sidebar-link.ativo`)
- Topbar com título da página e botões de ação contextuais (`.topbar-btn-*`)
- Hamburger menu responsivo para mobile (≤940px) com overlay escurecido
- JS compartilhado em `static/app.js`: `sidebarToggle()`, `abrirModal()`, `fecharModal()`, `criarToast()`
- Toast notifications lendo mensagens flash do Flask automaticamente no DOMContentLoaded
- Correção do favicon: retorna 204 se o arquivo não existir
- Badge de pendências nos Recorrentes visível na sidebar

## Rotas principais
- `GET /` — dashboard
- `GET /contador` — simulador fiscal
- `GET /gestao` — gestão de clientes, serviços, faturamentos
- `GET /notas` — notas fiscais
- `GET /notas/ver/<id>` — visualizar nota + seção de integração
- `GET /notas/gerar/<fat_id>` — gerar nota a partir de faturamento
- `POST /notas/criar` — criar nota manual
- `POST /notas/preparar/<id>` — preparar nota para integração
- `POST /notas/simular/<id>` — simular envio fiscal (protocolo fictício)
- `GET /notas/eventos/<id>` — histórico de eventos fiscais da nota
- `GET/POST /configuracao-fiscal` — configuração do emissor fiscal
- `GET/POST /integracao-fiscal` — configuração da integração NFS-e
- `GET /status-fiscal` — painel de status fiscal das notas
- `GET /backup` — baixar banco SQLite
- `GET /download-project` — baixar código-fonte
- `GET/POST /importar` — importar lançamentos via CSV (upload + prévia)
- `POST /importar/confirmar` — confirmar e salvar registros do CSV
- `GET /inteligencia` — motor de inteligência (score, tendências, perfil, alertas, comparação histórica, padrões, previsões, automações, plano de ação)
- `POST /previsao/registrar` — registra previsão de lançamento como entrada real

## Como rodar
```
python app.py
```
Acessa em: http://localhost:5000

## Workflow
- Start application: `python app.py` → porta 5000
