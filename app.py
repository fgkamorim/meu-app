import csv
import sqlite3
import os
import io
import json
import uuid
import zipfile
import shutil
import tempfile
import calendar
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, get_flashed_messages
from datetime import date, timedelta, datetime

app = Flask(__name__)
app.secret_key = "cf-etapa14-s3cr3t-2024"
DB = "database.db"

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS perfil_fiscal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo_atuacao TEXT NOT NULL,
            renda_media REAL DEFAULT 0,
            faturamento_estimado REAL DEFAULT 0,
            despesas_trabalho REAL DEFAULT 0,
            observacoes TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lancamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT NOT NULL,
            data TEXT NOT NULL,
            descricao TEXT NOT NULL,
            categoria TEXT NOT NULL,
            valor REAL NOT NULL,
            subtipo TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            valor_objetivo REAL NOT NULL,
            valor_atual REAL DEFAULT 0,
            prazo TEXT,
            descricao TEXT,
            tipo TEXT DEFAULT 'meta',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            tipo TEXT NOT NULL DEFAULT 'pf',
            cpf_cnpj TEXT,
            email TEXT,
            telefone TEXT,
            observacoes TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS servicos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            descricao TEXT,
            valor_padrao REAL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS faturamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id INTEGER NOT NULL,
            servico_id INTEGER NOT NULL,
            descricao TEXT,
            valor REAL NOT NULL,
            data TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pendente',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (cliente_id) REFERENCES clientes(id),
            FOREIGN KEY (servico_id) REFERENCES servicos(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS notas_fiscais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            faturamento_id INTEGER,
            cliente_id INTEGER NOT NULL,
            servico_id INTEGER NOT NULL,
            numero_nota TEXT NOT NULL,
            descricao TEXT,
            valor REAL NOT NULL,
            data_emissao TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'rascunho',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (faturamento_id) REFERENCES faturamentos(id),
            FOREIGN KEY (cliente_id) REFERENCES clientes(id),
            FOREIGN KEY (servico_id) REFERENCES servicos(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS configuracao_fiscal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            razao_social TEXT,
            cpf_cnpj TEXT,
            inscricao_municipal TEXT,
            inscricao_estadual TEXT,
            email TEXT,
            telefone TEXT,
            endereco TEXT,
            cidade TEXT,
            estado TEXT,
            cep TEXT,
            regime_tributario TEXT,
            observacoes TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS integracao_fiscal_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            modo_emissao TEXT NOT NULL DEFAULT 'interno',
            provedor TEXT,
            ambiente TEXT NOT NULL DEFAULT 'homologacao',
            municipio_codigo TEXT,
            observacoes TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS eventos_fiscais (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nota_id INTEGER NOT NULL,
            tipo_evento TEXT NOT NULL,
            status TEXT NOT NULL,
            mensagem TEXT,
            protocolo TEXT,
            payload_enviado TEXT,
            resposta_recebida TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (nota_id) REFERENCES notas_fiscais(id)
        )
    """)
    # Tabela de lançamentos recorrentes
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lancamentos_recorrentes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT NOT NULL,
            descricao TEXT NOT NULL,
            categoria TEXT NOT NULL,
            valor REAL NOT NULL,
            subtipo TEXT,
            frequencia TEXT NOT NULL DEFAULT 'mensal',
            proxima_data TEXT NOT NULL,
            ativo INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    # Tabela de padrões aprendidos (Etapa 16)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS padroes_aprendidos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo TEXT NOT NULL,
            chave_referencia TEXT NOT NULL,
            categoria_sugerida TEXT,
            subtipo_sugerido TEXT,
            valor_medio REAL DEFAULT 0,
            frequencia INTEGER DEFAULT 1,
            nivel_confianca TEXT DEFAULT 'baixa',
            ultima_ocorrencia TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    # Migrações: adicionar colunas novas em notas_fiscais se ainda não existirem
    for col_sql in [
        "ALTER TABLE notas_fiscais ADD COLUMN status_integracao TEXT NOT NULL DEFAULT 'nao_enviada'",
        "ALTER TABLE notas_fiscais ADD COLUMN payload_fiscal TEXT",
        "ALTER TABLE faturamentos ADD COLUMN lancamento_id INTEGER",
    ]:
        try:
            conn.execute(col_sql)
        except Exception:
            pass
    conn.commit()
    conn.close()

def get_periodo_dates(periodo):
    hoje = date.today()
    if periodo == "hoje":
        return hoje.isoformat(), hoje.isoformat()
    elif periodo == "7dias":
        return (hoje - timedelta(days=6)).isoformat(), hoje.isoformat()
    elif periodo == "30dias":
        return (hoje - timedelta(days=29)).isoformat(), hoje.isoformat()
    elif periodo == "mes":
        inicio = hoje.replace(day=1)
        return inicio.isoformat(), hoje.isoformat()
    return None, None

def calcular_inteligencia(total_entradas, total_saidas, saldo, total_fixas, total_variaveis):
    disponivel = max(0.0, saldo)
    sugerir_guardar = round(saldo * 0.20, 2) if saldo > 0 else 0.0
    sugerir_investir = round(saldo * 0.10, 2) if saldo > 0 else 0.0
    projecao = {"disponivel": disponivel, "sugerir_guardar": sugerir_guardar, "sugerir_investir": sugerir_investir}

    alertas = []
    if total_entradas == 0:
        alertas.append({"nivel": "warning", "msg": "Nenhuma entrada registrada no período."})
    else:
        if saldo < 0:
            alertas.append({"nivel": "danger", "msg": "Você gastou mais do que entrou no período."})
        if total_fixas > total_entradas * 0.50:
            pct = round(total_fixas / total_entradas * 100)
            alertas.append({"nivel": "warning", "msg": f"Suas despesas fixas estão altas ({pct}% da renda)."})
        if total_variaveis > total_entradas * 0.40:
            pct = round(total_variaveis / total_entradas * 100)
            alertas.append({"nivel": "warning", "msg": f"Seus gastos variáveis estão elevados ({pct}% da renda)."})
        if 0 <= saldo < total_entradas * 0.10:
            alertas.append({"nivel": "warning", "msg": "Seu saldo está muito baixo em relação às entradas."})
        if saldo >= total_entradas * 0.20:
            alertas.append({"nivel": "success", "msg": "Você pode guardar mais dinheiro se mantiver esse ritmo."})
        if saldo >= total_entradas * 0.30:
            alertas.append({"nivel": "success", "msg": "Excelente controle financeiro no período!"})

    frases = []
    if total_entradas == 0:
        frases.append("Registre suas entradas e despesas para receber análises personalizadas.")
    else:
        if saldo > total_entradas * 0.30:
            frases.append("Este mês você está controlando bem seus gastos.")
        elif saldo > 0:
            frases.append("Você está no positivo — mas há espaço para melhorar.")
        else:
            frases.append("Atenção: suas saídas superaram as entradas no período.")
        if total_fixas > total_entradas * 0.50:
            frases.append("Suas contas fixas consomem mais da metade da sua renda.")
        elif total_fixas > 0:
            frases.append("Suas despesas fixas estão dentro de um nível razoável.")
        if total_variaveis > total_entradas * 0.30:
            frases.append("Atenção com os gastos variáveis — eles estão pesando no orçamento.")
        if saldo > total_entradas * 0.20:
            frases.append("Você tem potencial para guardar dinheiro este mês.")

    return projecao, alertas, frases

def calcular_patrimonio(conn):
    # Saldo geral de todos os lançamentos
    todos = conn.execute("SELECT tipo, categoria, valor FROM lancamentos").fetchall()
    entradas_total = sum(l["valor"] for l in todos if l["tipo"] == "entrada")
    saidas_total  = sum(l["valor"] for l in todos if l["tipo"] == "despesa")
    saldo_geral   = entradas_total - saidas_total

    # Despesas fixas nos últimos 30 dias para base da reserva
    data_30 = (date.today() - timedelta(days=29)).isoformat()
    fixas_30 = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM lancamentos WHERE tipo='despesa' AND subtipo='fixa' AND data >= ?",
        (data_30,)
    ).fetchone()[0]

    # Metas
    metas = conn.execute("SELECT * FROM metas ORDER BY created_at DESC").fetchall()

    # Reserva de emergência (metas com tipo='reserva')
    guardado_reserva = sum(m["valor_atual"] for m in metas if m["tipo"] == "reserva")
    meta_reserva  = fixas_30 * 6  # 6 meses de despesas fixas
    falta_reserva = max(0.0, meta_reserva - guardado_reserva)
    pct_reserva   = round(min(100.0, (guardado_reserva / meta_reserva * 100) if meta_reserva > 0 else 0.0), 1)

    reserva = {
        "valor_guardado": guardado_reserva,
        "meta_sugerida": meta_reserva,
        "fixas_mes": fixas_30,
        "falta": falta_reserva,
        "pct": pct_reserva,
    }

    # Evolução patrimonial
    total_metas_acumulado = sum(m["valor_atual"] for m in metas)
    investido_metas       = sum(m["valor_atual"] for m in metas if m["tipo"] == "investimento")
    patrimonio_total      = saldo_geral + total_metas_acumulado

    patrimonial = {
        "saldo_conta": saldo_geral,
        "valor_guardado": total_metas_acumulado,
        "valor_investido": investido_metas,
        "patrimonio": patrimonio_total,
    }

    # Frases do resumo patrimonial
    frases_pat = []
    if not metas and saldo_geral == 0:
        frases_pat.append("Registre lançamentos e crie metas para ver seu resumo patrimonial.")
    else:
        concluidas = sum(1 for m in metas if m["valor_objetivo"] > 0 and m["valor_atual"] >= m["valor_objetivo"])
        if concluidas > 0:
            frases_pat.append(f"Parabéns! Você já concluiu {concluidas} meta(s) financeira(s).")
        if pct_reserva >= 100:
            frases_pat.append("Você está construindo uma boa reserva de emergência.")
        elif pct_reserva >= 50:
            frases_pat.append("Sua reserva de emergência está no caminho certo.")
        elif pct_reserva > 0:
            frases_pat.append("Sua reserva ainda está abaixo do ideal — continue contribuindo.")
        else:
            frases_pat.append("Atenção: você ainda não tem reserva de emergência registrada.")
        em_andamento = [m for m in metas if m["valor_objetivo"] > 0 and m["valor_atual"] < m["valor_objetivo"]]
        if em_andamento:
            frases_pat.append("Suas metas estão avançando — mantenha o ritmo.")
        if patrimonio_total > 0:
            frases_pat.append("Seu patrimônio está crescendo — continue assim!")
        if saldo_geral < 0:
            frases_pat.append("Atenção: você ainda depende muito da renda do mês para cobrir despesas.")

    return metas, reserva, patrimonial, frases_pat

# ── Motor de Inteligência Financeira ──────────────────────────────────────────

def _buscar_dados_mensais(conn, n_meses=6):
    """Retorna lista de dicts com dados agregados por mês (dos últimos n meses)."""
    today = date.today()
    resultado = []
    for i in range(n_meses - 1, -1, -1):
        year  = today.year
        month = today.month - i
        while month <= 0:
            month += 12
            year  -= 1
        mes_str = f"{year:04d}-{month:02d}"
        rows = conn.execute("""
            SELECT tipo, categoria, subtipo, SUM(valor) as total
            FROM lancamentos
            WHERE strftime('%Y-%m', data) = ?
            GROUP BY tipo, categoria, subtipo
        """, (mes_str,)).fetchall()
        entradas = 0.0
        saidas   = 0.0
        saidas_fixas   = 0.0
        saidas_variaveis = 0.0
        categorias_saida = {}
        for r in rows:
            if r["tipo"] == "entrada":
                entradas += r["total"]
            else:
                saidas += r["total"]
                if r["subtipo"] == "fixa":
                    saidas_fixas += r["total"]
                else:
                    saidas_variaveis += r["total"]
                cat = r["categoria"] or "Outros"
                categorias_saida[cat] = categorias_saida.get(cat, 0) + r["total"]
        resultado.append({
            "mes":            mes_str,
            "label":          f"{mes_str[5:]}/{mes_str[:4]}",
            "entradas":       entradas,
            "saidas":         saidas,
            "saldo":          entradas - saidas,
            "saidas_fixas":   saidas_fixas,
            "saidas_variaveis": saidas_variaveis,
            "categorias_saida": categorias_saida,
        })
    return resultado


def _calcular_score(mes_atual, media_entradas, media_saidas, taxa_poupanca, saldos_negativos, dados_mensais=None):
    """Calcula score financeiro 0–100 com fatores explicativos (histórico e tendência)."""
    score   = 50
    fatores = []

    if mes_atual["saldo"] > 0:
        score += 20
        fatores.append(("✅", "Saldo positivo no mês atual", +20))
    elif mes_atual["saldo"] < 0:
        score -= 20
        fatores.append(("❌", "Saldo negativo no mês atual", -20))

    if taxa_poupanca >= 20:
        score += 25
        fatores.append(("✅", f"Boa taxa de poupança ({taxa_poupanca:.0f}%)", +25))
    elif taxa_poupanca >= 10:
        score += 12
        fatores.append(("⚠", f"Taxa de poupança moderada ({taxa_poupanca:.0f}%)", +12))
    elif taxa_poupanca < 0:
        score -= 20
        fatores.append(("❌", f"Taxa de poupança negativa ({taxa_poupanca:.0f}%)", -20))
    else:
        score -= 5
        fatores.append(("⚠", f"Taxa de poupança baixa ({taxa_poupanca:.0f}%)", -5))

    ratio = (media_saidas / media_entradas * 100) if media_entradas > 0 else 100
    if ratio < 50:
        score += 10
        fatores.append(("✅", f"Despesas saudáveis ({ratio:.0f}% da renda média)", +10))
    elif ratio > 90:
        score -= 15
        fatores.append(("❌", f"Despesas muito altas ({ratio:.0f}% da renda média)", -15))
    elif ratio > 70:
        score -= 5
        fatores.append(("⚠", f"Despesas elevadas ({ratio:.0f}% da renda média)", -5))

    if saldos_negativos == 0:
        score += 5
        fatores.append(("✅", "Sem meses negativos recentes", +5))
    elif saldos_negativos >= 3:
        score -= 10
        fatores.append(("❌", f"{saldos_negativos} meses com saldo negativo", -10))
    elif saldos_negativos >= 2:
        score -= 5
        fatores.append(("⚠", f"{saldos_negativos} meses com saldo negativo", -5))

    # Fator de tendência (últimos 3 meses)
    if dados_mensais and len(dados_mensais) >= 3:
        ult3 = [m for m in dados_mensais[-3:] if m["entradas"] > 0 or m["saidas"] > 0]
        if len(ult3) >= 3:
            saldos3 = [m["saldo"] for m in ult3]
            if all(saldos3[i] <= saldos3[i+1] for i in range(len(saldos3)-1)) and saldos3[-1] > 0:
                score += 8
                fatores.append(("✅", "Saldo crescendo mês a mês (últimos 3 meses)", +8))
            elif all(s < 0 for s in saldos3):
                score -= 12
                fatores.append(("❌", "3 meses consecutivos com saldo negativo", -12))
            saldos_pos = sum(1 for s in saldos3 if s > 0)
            if saldos_pos == 3 and score < 80:
                score += 5
                fatores.append(("✅", "Todos os últimos 3 meses com saldo positivo", +5))

    return max(0, min(100, score)), fatores


def _detectar_erros(mes_atual, media_entradas, media_saidas, taxa_poupanca, saldos_negativos, categorias_total):
    """Detecta erros financeiros e retorna lista de dicts com titulo/explicacao/impacto/sugestao/nivel."""
    erros = []

    if media_entradas > 0:
        ratio = media_saidas / media_entradas * 100
        if ratio > 90:
            erros.append({
                "titulo":    "Despesas quase iguais à renda",
                "explicacao": f"Em média, você gasta {ratio:.0f}% de tudo que recebe.",
                "impacto":   "Sobra muito pouco ou nada no fim do período.",
                "sugestao":  "Revise as categorias de maior peso e elimine ao menos uma despesa variável.",
                "nivel":     "alto",
            })

        if taxa_poupanca < 5:
            erros.append({
                "titulo":    "Capacidade de guardar muito baixa",
                "explicacao": f"Você está guardando apenas {max(0, taxa_poupanca):.0f}% da sua renda.",
                "impacto":   "Sem reserva, qualquer imprevisto pode comprometer as finanças.",
                "sugestao":  "Tente poupar pelo menos 10% da renda antes de pagar outras despesas.",
                "nivel":     "medio",
            })

    if mes_atual["saldo"] < 0:
        erros.append({
            "titulo":    "Mês atual com saldo negativo",
            "explicacao": f"As despesas este mês superam as entradas em R$ {abs(mes_atual['saldo']):.2f}.",
            "impacto":   "Consumo de reservas ou risco de endividamento.",
            "sugestao":  "Identifique e elimine pelo menos uma despesa não essencial neste período.",
            "nivel":     "alto",
        })

    if saldos_negativos >= 3:
        erros.append({
            "titulo":    "Padrão de déficit recorrente",
            "explicacao": f"{saldos_negativos} dos últimos meses fecharam negativos.",
            "impacto":   "Tendência de endividamento progressivo.",
            "sugestao":  "Revise o orçamento completo — pode haver despesas fixas desnecessárias.",
            "nivel":     "alto",
        })

    if categorias_total:
        total_saidas = sum(categorias_total.values())
        if total_saidas > 0:
            cat_maior_nome, cat_maior_val = max(categorias_total.items(), key=lambda x: x[1])
            if (cat_maior_val / total_saidas) > 0.45:
                erros.append({
                    "titulo":    f"Concentração em '{cat_maior_nome}'",
                    "explicacao": f"A categoria representa {cat_maior_val/total_saidas*100:.0f}% de todos os gastos.",
                    "impacto":   "Dependência excessiva de uma única área.",
                    "sugestao":  f"Revise os gastos em '{cat_maior_nome}' para identificar oportunidades de redução.",
                    "nivel":     "medio",
                })

    return erros


def _gerar_alertas(mes_atual, mes_anterior, media_entradas, media_saidas, taxa_poupanca):
    """Gera alertas contextuais com prioridade baixo/medio/alto."""
    alertas = []

    if mes_anterior and mes_anterior["saidas"] > 0:
        var_saidas = (mes_atual["saidas"] - mes_anterior["saidas"]) / mes_anterior["saidas"] * 100
        if var_saidas > 20:
            alertas.append({
                "texto":      f"Seus gastos este mês estão {var_saidas:.0f}% acima do mês anterior.",
                "prioridade": "alto" if var_saidas > 40 else "medio",
            })

    if mes_anterior and mes_anterior["entradas"] > 0:
        var_ent = (mes_atual["entradas"] - mes_anterior["entradas"]) / mes_anterior["entradas"] * 100
        if var_ent < -20:
            alertas.append({
                "texto":      f"Sua renda este mês caiu {abs(var_ent):.0f}% em relação ao mês anterior.",
                "prioridade": "alto",
            })

    if mes_atual["saldo"] < 0:
        alertas.append({
            "texto":      "Seu saldo do mês está negativo. Revise os gastos antes do período acabar.",
            "prioridade": "alto",
        })

    if taxa_poupanca < 10 and media_entradas > 0:
        alertas.append({
            "texto":      f"Você está guardando menos de 10% da renda (atual: {max(0, taxa_poupanca):.0f}%).",
            "prioridade": "medio",
        })

    if media_entradas > 0 and mes_atual["entradas"] < media_entradas * 0.7:
        alertas.append({
            "texto":      f"Renda deste mês está abaixo da média habitual (R$ {media_entradas:.2f}).",
            "prioridade": "medio",
        })

    if mes_atual["entradas"] > 0 and mes_atual["saidas_fixas"] / mes_atual["entradas"] > 0.5:
        alertas.append({
            "texto":      f"Despesas fixas consumindo {mes_atual['saidas_fixas']/mes_atual['entradas']*100:.0f}% da renda do mês.",
            "prioridade": "medio",
        })

    if not alertas and taxa_poupanca >= 20:
        alertas.append({
            "texto":      "Suas finanças estão em bom equilíbrio. Continue assim!",
            "prioridade": "baixo",
        })

    return alertas[:6]


def _gerar_sugestoes(mes_atual, media_entradas, media_saidas, taxa_poupanca, categorias_total):
    """Gera sugestões automáticas baseadas nos dados reais."""
    sugestoes = []

    if categorias_total:
        cat_maior_nome, cat_maior_val = max(categorias_total.items(), key=lambda x: x[1])
        economia = cat_maior_val * 0.15
        sugestoes.append({
            "icone": "💡",
            "texto": f"Reduza 15% dos gastos em '{cat_maior_nome}' e libere aproximadamente R$ {economia:.0f} por período.",
        })

    if taxa_poupanca < 20 and media_entradas > 0:
        meta20 = media_entradas * 0.20
        guardado = max(0.0, media_entradas - media_saidas)
        diff = meta20 - guardado
        if diff > 0:
            sugestoes.append({
                "icone": "🎯",
                "texto": f"Para guardar 20% da renda, você precisa cortar R$ {diff:.0f} nas despesas mensais.",
            })

    if mes_atual["saldo"] > 0:
        sugestoes.append({
            "icone": "🏦",
            "texto": f"Você tem R$ {mes_atual['saldo']:.2f} de saldo positivo este mês — considere reforçar a reserva de emergência.",
        })

    if media_entradas > 0 and media_saidas / media_entradas > 0.80:
        sugestoes.append({
            "icone": "⚖",
            "texto": "Revise as despesas fixas recorrentes — podem estar consumindo mais do que o ideal.",
        })

    if mes_atual["saidas_variaveis"] > mes_atual["saidas_fixas"] * 1.5:
        sugestoes.append({
            "icone": "📉",
            "texto": "Seus gastos variáveis são muito maiores que os fixos — avalie onde está o excesso.",
        })

    sugestoes.append({
        "icone": "📊",
        "texto": "Categorize todos os lançamentos para que o sistema gere análises cada vez mais precisas.",
    })

    return sugestoes[:5]


def _gerar_resumo(mes_atual, erros, sugestoes, taxa_poupanca, score, tendencias=None):
    """Monta o resumo executivo automático com 4 blocos."""
    if mes_atual["saldo"] > 0:
        situacao = f"Mês positivo com saldo de R$ {mes_atual['saldo']:.2f}."
    elif mes_atual["saldo"] < 0:
        situacao = f"Mês negativo — déficit de R$ {abs(mes_atual['saldo']):.2f} no período atual."
    else:
        situacao = "Saldo equilibrado no mês, sem margem de poupança."
    if taxa_poupanca >= 15:
        situacao += " Boa capacidade de guardar."
    elif taxa_poupanca < 0:
        situacao += " Gastos superando a renda."

    erros_altos = [e for e in erros if e["nivel"] == "alto"]
    if erros_altos:
        risco = erros_altos[0]["titulo"] + " — " + erros_altos[0]["explicacao"]
    elif erros:
        risco = erros[0]["titulo"] + " — " + erros[0]["explicacao"]
    else:
        risco = "Nenhum risco crítico identificado no momento."

    oportunidade = sugestoes[0]["texto"] if sugestoes else "Continue registrando lançamentos para análise mais precisa."

    if score < 40:
        acao = "Revise urgentemente as despesas fixas e elimine gastos não essenciais."
    elif score < 60:
        acao = "Foque em aumentar a taxa de poupança e regularizar o orçamento mensal."
    elif score < 80:
        acao = "Mantenha o ritmo atual e avalie reforçar a reserva de emergência."
    else:
        acao = "Situação saudável. Considere direcionar o excedente para metas ou investimentos."

    # Contexto de tendência
    tendencia_texto = ""
    if tendencias:
        if tendencias["tend_saldo"] == "subindo":
            tendencia_texto = "Sua saúde financeira está melhorando nos últimos meses."
        elif tendencias["tend_saldo"] == "caindo":
            tendencia_texto = "Atenção: seu saldo vem caindo mês a mês."
        if tendencias["neg_seguidos"] >= 2:
            tendencia_texto += f" {tendencias['neg_seguidos']} meses seguidos com déficit — revisão urgente."
        if tendencias["tend_renda"] == "subindo" and tendencias["tend_gastos"] == "caindo":
            tendencia_texto = "Excelente: renda crescendo e gastos reduzindo. Continue assim!"
    return {"situacao": situacao, "risco": risco, "oportunidade": oportunidade, "acao": acao, "tendencia": tendencia_texto}


# ── Etapa 16: Motor de Padrões, Previsões, Automações ────────────────────────

def _detectar_padroes(conn):
    """Analisa histórico e detecta padrões recorrentes de lançamentos."""
    lancamentos = conn.execute("""
        SELECT tipo, descricao, categoria, subtipo, valor,
               CAST(strftime('%d', data) AS INTEGER) as dia,
               strftime('%Y-%m', data) as mes_ano
        FROM lancamentos ORDER BY data DESC LIMIT 600
    """).fetchall()
    if not lancamentos:
        return []

    grupos = {}
    for l in lancamentos:
        chave = l["descricao"][:20].strip().lower()
        if chave not in grupos:
            grupos[chave] = []
        grupos[chave].append(l)

    padroes = []
    for chave, itens in grupos.items():
        if len(itens) < 2:
            continue
        valores  = [i["valor"] for i in itens]
        vm       = sum(valores) / len(valores)
        v_std    = (sum((v - vm)**2 for v in valores) / len(valores)) ** 0.5
        dias     = [i["dia"] for i in itens]
        dm       = sum(dias) / len(dias)
        d_std    = (sum((d - dm)**2 for d in dias) / len(dias)) ** 0.5
        tipos    = [i["tipo"] for i in itens]
        cats     = [i["categoria"] for i in itens]
        subs     = [i["subtipo"] or "" for i in itens]
        tipo_dom = max(set(tipos), key=tipos.count)
        cat_dom  = max(set(cats),  key=cats.count)
        sub_dom  = max(set(subs),  key=subs.count)
        meses    = set(i["mes_ano"] for i in itens)
        freq     = len(itens)
        val_ok   = (v_std / vm < 0.25) if vm > 0 else False
        dia_ok   = d_std < 6
        if freq >= 4 and val_ok and dia_ok:
            confianca = "alta"
        elif freq >= 3 and (val_ok or dia_ok):
            confianca = "media"
        elif freq >= 2:
            confianca = "baixa"
        else:
            continue
        padroes.append({
            "descricao":  itens[0]["descricao"][:40],
            "desc_curta": chave[:25],
            "categoria":  cat_dom,
            "subtipo":    sub_dom,
            "tipo":       tipo_dom,
            "valor_medio": round(vm, 2),
            "valor_std":  round(v_std, 2),
            "frequencia": freq,
            "dia_medio":  int(dm),
            "dia_std":    round(d_std, 1),
            "confianca":  confianca,
            "n_meses":    len(meses),
        })
        _atualizar_padrao_aprendido(conn, chave, cat_dom, sub_dom, vm, freq, confianca)

    padroes.sort(key=lambda x: ({"alta": 3, "media": 2, "baixa": 1}[x["confianca"]], x["frequencia"]), reverse=True)
    return padroes[:12]


def _atualizar_padrao_aprendido(conn, chave, categoria, subtipo, valor_medio, frequencia, confianca):
    """Atualiza ou insere padrão aprendido no banco."""
    existente = conn.execute(
        "SELECT id FROM padroes_aprendidos WHERE chave_referencia=? LIMIT 1", (chave,)
    ).fetchone()
    hoje = date.today().isoformat()
    if existente:
        conn.execute("""
            UPDATE padroes_aprendidos SET
                categoria_sugerida=?, subtipo_sugerido=?, valor_medio=?,
                frequencia=?, nivel_confianca=?, ultima_ocorrencia=?
            WHERE chave_referencia=?
        """, (categoria, subtipo, round(valor_medio, 2), frequencia, confianca, hoje, chave))
    else:
        conn.execute("""
            INSERT INTO padroes_aprendidos
                (tipo, chave_referencia, categoria_sugerida, subtipo_sugerido,
                 valor_medio, frequencia, nivel_confianca, ultima_ocorrencia)
            VALUES ('lancamento', ?, ?, ?, ?, ?, ?, ?)
        """, (chave, categoria, subtipo, round(valor_medio, 2), frequencia, confianca, hoje))


def _gerar_previsoes(conn, padroes):
    """Gera previsões de lançamentos prováveis no mês atual com base nos padrões."""
    hoje       = date.today()
    mes_atual  = hoje.strftime("%Y-%m")
    existentes = conn.execute(
        "SELECT descricao FROM lancamentos WHERE strftime('%Y-%m', data)=?", (mes_atual,)
    ).fetchall()
    desc_existentes = set(e["descricao"][:20].lower().strip() for e in existentes)

    previsoes = []
    for p in padroes:
        if p["confianca"] == "baixa" or p["n_meses"] < 2:
            continue
        ja_existe = p["desc_curta"] in desc_existentes or any(
            p["desc_curta"][:15] in e for e in desc_existentes
        )
        if ja_existe:
            continue
        dia_est  = max(1, min(p["dia_medio"], 28))
        data_est = f"{hoje.year}-{hoje.month:02d}-{dia_est:02d}"
        previsoes.append({
            "descricao":     p["descricao"],
            "categoria":     p["categoria"],
            "subtipo":       p["subtipo"],
            "tipo":          p["tipo"],
            "valor_estimado": p["valor_medio"],
            "data_estimada": data_est,
            "confianca":     p["confianca"],
            "frequencia":    p["frequencia"],
        })
    return previsoes[:8]


def _alertas_preventivos(conn, dados_mensais, mes_atual_d):
    """Gera alertas preventivos com projeções de fim de mês."""
    alertas = []
    hoje     = date.today()
    dias_mes = calendar.monthrange(hoje.year, hoje.month)[1]
    dia_atual = hoje.day
    if dia_atual < 5 or mes_atual_d["entradas"] == 0:
        return alertas

    # 1. Projeção de saldo ao fim do mês
    if mes_atual_d["saidas"] > 0:
        taxa_diaria = mes_atual_d["saidas"] / dia_atual
        gasto_proj  = taxa_diaria * dias_mes
        saldo_proj  = mes_atual_d["entradas"] - gasto_proj
        if saldo_proj < 0:
            alertas.append({
                "texto": f"No ritmo atual, você pode terminar o mês com saldo negativo (projeção: R$ {saldo_proj:.0f}).",
                "prioridade": "alto", "icone": "📉", "nivel": "alto",
            })
        elif saldo_proj < mes_atual_d["entradas"] * 0.08:
            alertas.append({
                "texto": f"Projeção de saldo para fim do mês é apenas R$ {saldo_proj:.0f} — menos de 8% da renda.",
                "prioridade": "medio", "icone": "⚠", "nivel": "medio",
            })

    # 2. Mês caminhando pior que o anterior
    if len(dados_mensais) >= 2:
        mes_ant = dados_mensais[-2]
        if mes_ant["saidas"] > 0 and dia_atual >= 10:
            proporcao = dia_atual / dias_mes
            saidas_proj = mes_atual_d["saidas"] / proporcao if proporcao > 0 else 0
            if saidas_proj > mes_ant["saidas"] * 1.25:
                delta = ((saidas_proj / mes_ant["saidas"]) - 1) * 100
                alertas.append({
                    "texto": f"Este mês está caminhando para gastos {delta:.0f}% maiores que o mês anterior.",
                    "prioridade": "medio", "icone": "📊", "nivel": "medio",
                })

    return alertas


def _gerar_automacoes(conn, padroes, previsoes, dados):
    """Gera sugestões de automação inteligente com base no contexto atual."""
    automacoes = []

    # 1. Sugerir criar recorrente para padrão forte ainda não automatizado
    recs = conn.execute("SELECT descricao FROM lancamentos_recorrentes").fetchall()
    desc_recs = set(r["descricao"][:15].lower() for r in recs)
    for p in padroes:
        if p["confianca"] == "alta" and p["frequencia"] >= 4:
            if not any(p["desc_curta"][:15] in dr for dr in desc_recs):
                automacoes.append({
                    "tipo": "criar_recorrente",
                    "icone": "🔄",
                    "titulo": "Automatizar lançamento recorrente",
                    "descricao": f"'{p['descricao'][:35]}' ocorreu {p['frequencia']}× — considere automatizar como recorrente.",
                    "acao_url": "/recorrentes",
                    "acao_label": "Ver recorrentes",
                    "prioridade": "alta",
                })
                break

    # 2. Registrar previsão pendente de alta confiança
    for prev in previsoes:
        if prev["confianca"] == "alta":
            automacoes.append({
                "tipo": "registrar_previsao",
                "icone": "📋",
                "titulo": "Registrar despesa prevista",
                "descricao": f"'{prev['descricao'][:35]}' ainda não foi lançada este mês. Valor esperado: R$ {prev['valor_estimado']:.0f}.",
                "acao_url": "/?tipo=" + prev["tipo"] + "&desc=" + prev["descricao"][:20] + "&valor=" + str(prev["valor_estimado"]) + "&cat=" + prev["categoria"],
                "acao_label": "Registrar agora",
                "prioridade": "alta",
                "dados_prev": prev,
            })
            break

    # 3. Avançar meta quando há saldo positivo
    mes_atual = dados.get("mes_atual", {})
    if mes_atual.get("saldo", 0) > 50:
        meta = conn.execute(
            "SELECT id, nome, valor_objetivo, valor_atual FROM metas WHERE tipo='meta' AND valor_atual < valor_objetivo ORDER BY id LIMIT 1"
        ).fetchone()
        if meta:
            automacoes.append({
                "tipo": "atualizar_meta",
                "icone": "🎯",
                "titulo": "Reforçar meta financeira",
                "descricao": f"Há saldo de R$ {mes_atual['saldo']:.0f} disponível. Avance na meta '{meta['nome']}'.",
                "acao_url": "/gestao#sec-metas",
                "acao_label": "Ver metas",
                "prioridade": "media",
            })

    # 4. Gerar nota para faturamento pago sem nota
    fat = conn.execute("""
        SELECT f.id, c.nome as cliente, f.valor
        FROM faturamentos f JOIN clientes c ON c.id=f.cliente_id
        LEFT JOIN notas_fiscais n ON n.faturamento_id=f.id
        WHERE f.status='pago' AND n.id IS NULL LIMIT 1
    """).fetchone()
    if fat:
        automacoes.append({
            "tipo": "gerar_nota",
            "icone": "🧾",
            "titulo": "Gerar nota fiscal pendente",
            "descricao": f"Faturamento de R$ {fat['valor']:.0f} ({fat['cliente']}) está pago mas sem nota gerada.",
            "acao_url": f"/notas/gerar/{fat['id']}",
            "acao_label": "Gerar nota",
            "prioridade": "media",
        })

    # 5. Transformar faturamento pago em entrada
    fat_sem = conn.execute(
        "SELECT COUNT(*) as n FROM faturamentos WHERE status='pago' AND lancamento_id IS NULL"
    ).fetchone()
    if fat_sem and fat_sem["n"] > 0:
        automacoes.append({
            "tipo": "faturamento_entrada",
            "icone": "💰",
            "titulo": "Vincular recebimento ao financeiro",
            "descricao": f"{fat_sem['n']} faturamento(s) pago(s) ainda não registrado(s) como entrada no controle financeiro.",
            "acao_url": "/gestao#sec-faturamentos",
            "acao_label": "Ver faturamentos",
            "prioridade": "media",
        })

    return automacoes[:5]


def _gerar_plano_acao(dados, previsoes, automacoes):
    """Monta plano de ação com até 5 itens prioritários ordenados por importância."""
    acoes = []

    # Alertas críticos
    for a in dados.get("alertas", []):
        if a.get("prioridade") == "alto" and len(acoes) < 2:
            acoes.append({
                "ordem": 1, "icone": "🚨",
                "titulo": "Resolver alerta urgente",
                "descricao": a["texto"][:120],
                "url": "/inteligencia", "label": "Ver análise completa",
            })

    # Previsões pendentes de alta confiança
    for prev in previsoes:
        if prev["confianca"] == "alta" and len(acoes) < 4:
            acoes.append({
                "ordem": 2, "icone": "📋",
                "titulo": f"Registrar: {prev['descricao'][:30]}",
                "descricao": f"Previsto para dia {prev['data_estimada'][8:10]} — R$ {prev['valor_estimado']:.0f} ({prev['tipo']})",
                "url": "/", "label": "Ir ao dashboard",
            })
            break

    # Erros de nível alto
    for e in dados.get("erros", []):
        if e["nivel"] == "alto" and len(acoes) < 4:
            acoes.append({
                "ordem": 3, "icone": "🔍",
                "titulo": e["titulo"],
                "descricao": e["sugestao"][:120],
                "url": "/inteligencia", "label": "Ver detector",
            })

    # Automações disponíveis
    for a in automacoes:
        if a["prioridade"] in ("alta", "media") and len(acoes) < 5:
            acoes.append({
                "ordem": 4, "icone": a["icone"],
                "titulo": a["titulo"],
                "descricao": a["descricao"][:120],
                "url": a["acao_url"], "label": a["acao_label"],
            })

    # Sugestão genérica se nenhum passo encontrado
    if not acoes:
        acoes.append({
            "ordem": 5, "icone": "💡",
            "titulo": "Continue registrando lançamentos",
            "descricao": "Quanto mais dados, mais precisas ficam as análises, previsões e alertas.",
            "url": "/", "label": "Registrar lançamento",
        })

    return sorted(acoes, key=lambda x: x["ordem"])[:5]


def calcular_inteligencia_financeira(conn):
    """Função central do motor de inteligência — retorna todos os insights."""
    dados_mensais    = _buscar_dados_mensais(conn, 6)
    meses_com_dados  = [m for m in dados_mensais if m["entradas"] > 0 or m["saidas"] > 0]
    n_meses          = len(meses_com_dados)

    if n_meses == 0:
        return {"sem_dados": True, "poucos_dados": True}

    media_entradas = sum(m["entradas"] for m in meses_com_dados) / n_meses
    media_saidas   = sum(m["saidas"]   for m in meses_com_dados) / n_meses
    media_saldo    = media_entradas - media_saidas
    taxa_poupanca  = ((media_entradas - media_saidas) / media_entradas * 100) if media_entradas > 0 else 0

    mes_atual    = dados_mensais[-1]
    mes_anterior = dados_mensais[-2] if len(dados_mensais) >= 2 else None
    saldos_negativos = sum(1 for m in meses_com_dados if m["saldo"] < 0)

    # Categorias agregadas (todas as despesas dos meses com dados)
    categorias_total = {}
    for m in meses_com_dados:
        for cat, val in m["categorias_saida"].items():
            categorias_total[cat] = categorias_total.get(cat, 0) + val

    cats_sorted = sorted(categorias_total.items(), key=lambda x: x[1], reverse=True)
    cat_maior   = cats_sorted[0][0] if cats_sorted else None

    # Crescimento de categoria (mês atual vs média dos demais)
    cat_crescimento = None
    if mes_atual["categorias_saida"] and len(meses_com_dados) > 1:
        outros = meses_com_dados[:-1]
        for cat, val_atual in mes_atual["categorias_saida"].items():
            media_cat = sum(m["categorias_saida"].get(cat, 0) for m in outros) / len(outros)
            if media_cat > 0 and val_atual > media_cat * 1.3:
                cat_crescimento = (cat, val_atual, media_cat, (val_atual - media_cat) / media_cat * 100)
                break

    score, fatores_score = _calcular_score(mes_atual, media_entradas, media_saidas, taxa_poupanca, saldos_negativos, dados_mensais)
    erros     = _detectar_erros(mes_atual, media_entradas, media_saidas, taxa_poupanca, saldos_negativos, categorias_total)
    sugestoes = _gerar_sugestoes(mes_atual, media_entradas, media_saidas, taxa_poupanca, categorias_total)
    tendencias = _calcular_tendencias(dados_mensais)
    comparacao = _calcular_comparacao_historica(dados_mensais)
    perfil     = _calcular_perfil_financeiro(meses_com_dados, media_entradas, media_saidas)
    alertas    = _gerar_alertas_historicos(dados_mensais, mes_atual, mes_anterior, media_entradas, media_saidas, taxa_poupanca)
    resumo    = _gerar_resumo(mes_atual, erros, sugestoes, taxa_poupanca, score, tendencias)

    # Etapa 16: Motor avançado
    padroes    = _detectar_padroes(conn)
    previsoes  = _gerar_previsoes(conn, padroes)
    alertas_prev = _alertas_preventivos(conn, dados_mensais, mes_atual)
    alertas    = alertas_prev + alertas  # preventivos primeiro
    alertas    = alertas[:7]

    dados_para_automacao = {
        "mes_atual": mes_atual, "erros": erros, "alertas": alertas, "sugestoes": sugestoes,
    }
    automacoes = _gerar_automacoes(conn, padroes, previsoes, dados_para_automacao)
    plano_acao = _gerar_plano_acao(
        {"alertas": alertas, "erros": erros, "sugestoes": sugestoes},
        previsoes, automacoes
    )
    conn.commit()

    return {
        "sem_dados":        False,
        "poucos_dados":     n_meses < 2,
        "n_meses":          n_meses,
        "dados_mensais":    dados_mensais,
        "meses_com_dados":  meses_com_dados,
        "media_entradas":   media_entradas,
        "media_saidas":     media_saidas,
        "media_saldo":      media_saldo,
        "taxa_poupanca":    taxa_poupanca,
        "mes_atual":        mes_atual,
        "mes_anterior":     mes_anterior,
        "cats_sorted":      cats_sorted,
        "cat_maior":        cat_maior,
        "cat_crescimento":  cat_crescimento,
        "saldos_negativos": saldos_negativos,
        "score":            score,
        "fatores_score":    fatores_score,
        "erros":            erros,
        "alertas":          alertas,
        "sugestoes":        sugestoes,
        "resumo":           resumo,
        "tendencias":       tendencias,
        "comparacao":       comparacao,
        "perfil":           perfil,
        # Etapa 16
        "padroes":          padroes,
        "previsoes":        previsoes,
        "automacoes":       automacoes,
        "plano_acao":       plano_acao,
    }


def faixa_score(score):
    if score >= 80: return ("Excelente", "score-excelente")
    if score >= 60: return ("Boa", "score-boa")
    if score >= 40: return ("Atenção", "score-atencao")
    return ("Risco elevado", "score-risco")


# ── Etapa 15: Funções de análise histórica e tendência ────────────────────────

def _calcular_comparacao_historica(dados_mensais):
    """Compara mês atual vs anterior e vs média histórica."""
    if len(dados_mensais) < 2:
        return None
    mes_atual = dados_mensais[-1]
    historico = dados_mensais[:-1]
    meses_hist = [m for m in historico if m["entradas"] > 0 or m["saidas"] > 0]
    n = len(meses_hist)
    if n == 0:
        return None
    mes_ant = dados_mensais[-2]

    def var_pct(novo, velho):
        return ((novo - velho) / velho * 100) if velho > 0 else None

    media_ent = sum(m["entradas"] for m in meses_hist) / n
    media_sai = sum(m["saidas"]   for m in meses_hist) / n

    # Comparação de categorias vs média histórica
    all_cats = set(mes_atual["categorias_saida"].keys())
    for m in meses_hist:
        all_cats.update(m["categorias_saida"].keys())

    cat_comparacao = []
    for cat in all_cats:
        val_atual  = mes_atual["categorias_saida"].get(cat, 0)
        media_cat  = sum(m["categorias_saida"].get(cat, 0) for m in meses_hist) / n
        variacao   = var_pct(val_atual, media_cat) if media_cat > 0 else (100.0 if val_atual > 0 else 0.0)
        cat_comparacao.append({"nome": cat, "atual": val_atual, "media": media_cat, "variacao": variacao})

    cat_comparacao.sort(key=lambda x: abs(x["variacao"]), reverse=True)

    return {
        "mes_atual_label": mes_atual["label"],
        "mes_ant_label":   mes_ant["label"],
        "n_meses_hist":    n,
        "ent_atual": mes_atual["entradas"], "sai_atual": mes_atual["saidas"], "saldo_atual": mes_atual["saldo"],
        "ent_ant":   mes_ant["entradas"],   "sai_ant":   mes_ant["saidas"],   "saldo_ant":   mes_ant["saldo"],
        "media_ent": media_ent, "media_sai": media_sai,
        "var_ent_ant":   var_pct(mes_atual["entradas"], mes_ant["entradas"]),
        "var_sai_ant":   var_pct(mes_atual["saidas"],   mes_ant["saidas"]),
        "var_sal_ant":   var_pct(mes_atual["saldo"],    abs(mes_ant["saldo"])) if mes_ant["saldo"] != 0 else None,
        "var_ent_media": var_pct(mes_atual["entradas"], media_ent),
        "var_sai_media": var_pct(mes_atual["saidas"],   media_sai),
        "cat_comparacao": cat_comparacao[:6],
    }


def _calcular_tendencias(dados_mensais):
    """Analisa tendência de saldo, renda e gastos nos últimos meses."""
    validos = [m for m in dados_mensais if m["entradas"] > 0 or m["saidas"] > 0]
    if len(validos) < 2:
        return None

    ult3 = validos[-3:] if len(validos) >= 3 else validos

    def tend(vals):
        if len(vals) < 2 or vals[0] == 0:
            return "estável", 0.0
        pct = (vals[-1] - vals[0]) / abs(vals[0]) * 100
        if pct > 10:  return "subindo", pct
        if pct < -10: return "caindo",  abs(pct)
        return "estável", abs(pct)

    ents   = [m["entradas"] for m in ult3]
    sais   = [m["saidas"]   for m in ult3]
    saldos = [m["saldo"]    for m in ult3]

    tend_renda,  pct_renda  = tend(ents)
    tend_gastos, pct_gastos = tend(sais)
    tend_saldo,  pct_saldo  = tend(saldos)

    # Categorias crescendo de forma contínua
    all_cats = set()
    for m in ult3:
        all_cats.update(m["categorias_saida"].keys())
    cats_em_alta = []
    for cat in all_cats:
        vals = [m["categorias_saida"].get(cat, 0) for m in ult3]
        if len(vals) >= 2 and vals[0] > 0 and all(vals[i] <= vals[i+1] for i in range(len(vals)-1)):
            pct = (vals[-1] - vals[0]) / vals[0] * 100
            if pct > 10:
                cats_em_alta.append({"nome": cat, "pct": pct, "valor": vals[-1]})
    cats_em_alta.sort(key=lambda x: x["pct"], reverse=True)

    neg_seguidos = 0
    for m in reversed(validos):
        if m["saldo"] < 0: neg_seguidos += 1
        else: break

    return {
        "tend_renda":  tend_renda,  "pct_renda":  round(pct_renda, 1),
        "tend_gastos": tend_gastos, "pct_gastos": round(pct_gastos, 1),
        "tend_saldo":  tend_saldo,  "pct_saldo":  round(pct_saldo, 1),
        "ult3_labels":   [m["label"]    for m in ult3],
        "ult3_entradas": ents,
        "ult3_saidas":   sais,
        "ult3_saldos":   saldos,
        "cats_em_alta":  cats_em_alta[:3],
        "neg_seguidos":  neg_seguidos,
        "n_meses":       len(ult3),
    }


def _calcular_perfil_financeiro(meses_com_dados, media_entradas, media_saidas):
    """Calcula o perfil financeiro com base no histórico."""
    if not meses_com_dados:
        return None
    n = len(meses_com_dados)
    cap_guardar = media_entradas - media_saidas
    pct_guardar = (cap_guardar / media_entradas * 100) if media_entradas > 0 else 0.0

    total_fixas     = sum(m["saidas_fixas"]     for m in meses_com_dados)
    total_variaveis = sum(m["saidas_variaveis"]  for m in meses_com_dados)
    total_saidas_h  = total_fixas + total_variaveis
    pct_fixas     = (total_fixas     / total_saidas_h * 100) if total_saidas_h > 0 else 0.0
    pct_variaveis = (total_variaveis / total_saidas_h * 100) if total_saidas_h > 0 else 0.0

    cats_total = {}
    for m in meses_com_dados:
        for cat, val in m["categorias_saida"].items():
            cats_total[cat] = cats_total.get(cat, 0) + val
    total_cat_sum = sum(cats_total.values())
    top_cats = []
    for cat, val in sorted(cats_total.items(), key=lambda x: x[1], reverse=True)[:6]:
        pct = (val / total_cat_sum * 100) if total_cat_sum > 0 else 0
        top_cats.append({"nome": cat, "media_mensal": val / n, "pct": round(pct, 1), "barra": min(100, int(pct))})

    if pct_guardar >= 30:  perfil_label, perfil_cor = "Poupador Consistente", "verde"
    elif pct_guardar >= 15: perfil_label, perfil_cor = "Boa Gestão",           "azul"
    elif pct_guardar >= 5:  perfil_label, perfil_cor = "Atenção Necessária",   "amarelo"
    else:                   perfil_label, perfil_cor = "Em Situação de Risco", "vermelho"

    return {
        "n_meses": n, "media_renda": media_entradas, "media_gastos": media_saidas,
        "cap_guardar": cap_guardar, "pct_guardar": pct_guardar,
        "pct_fixas": pct_fixas, "pct_variaveis": pct_variaveis,
        "top_cats": top_cats, "perfil_label": perfil_label, "perfil_cor": perfil_cor,
    }


def _gerar_alertas_historicos(dados_mensais, mes_atual, mes_anterior, media_entradas, media_saidas, taxa_poupanca):
    """Alertas personalizados baseados no histórico real."""
    alertas = []
    validos = [m for m in dados_mensais if m["entradas"] > 0 or m["saidas"] > 0]

    # 1. Gastos acima do padrão histórico
    if media_saidas > 0 and mes_atual["saidas"] > 0:
        var = (mes_atual["saidas"] - media_saidas) / media_saidas * 100
        if var > 25:
            alertas.append({
                "texto": f"Você está gastando {var:.0f}% acima do seu padrão normal (atual R$ {mes_atual['saidas']:.0f} vs média R$ {media_saidas:.0f}).",
                "prioridade": "alto" if var > 50 else "medio", "nivel": "alto" if var > 50 else "medio",
                "icone": "📊",
            })

    # 2. Queda de renda vs mês anterior
    if mes_anterior and mes_anterior["entradas"] > 0:
        var_ent = (mes_atual["entradas"] - mes_anterior["entradas"]) / mes_anterior["entradas"] * 100
        if var_ent < -20:
            alertas.append({
                "texto": f"Sua renda caiu {abs(var_ent):.0f}% em relação ao mês anterior (de R$ {mes_anterior['entradas']:.0f} para R$ {mes_atual['entradas']:.0f}).",
                "prioridade": "alto", "nivel": "alto", "icone": "📉",
            })

    # 3. Meses negativos consecutivos
    neg_seq = 0
    for m in reversed(validos):
        if m["saldo"] < 0: neg_seq += 1
        else: break
    if neg_seq >= 2:
        alertas.append({
            "texto": f"Alerta: {neg_seq} meses seguidos com saldo negativo. Revisão urgente do orçamento necessária.",
            "prioridade": "alto", "nivel": "alto", "icone": "🚨",
        })

    # 4. Categoria crescendo continuamente por 3 meses
    if len(dados_mensais) >= 3:
        all_cats = set()
        for m in dados_mensais[-3:]: all_cats.update(m["categorias_saida"].keys())
        for cat in all_cats:
            vals = [m["categorias_saida"].get(cat, 0) for m in dados_mensais[-3:]]
            if vals[0] > 0 and len(vals) == 3 and vals[0] < vals[1] < vals[2]:
                pct = (vals[2] - vals[0]) / vals[0] * 100
                if pct > 20:
                    alertas.append({
                        "texto": f"'{cat.capitalize()}' cresceu {pct:.0f}% nos últimos 3 meses ({vals[0]:.0f} → {vals[1]:.0f} → {vals[2]:.0f} R$).",
                        "prioridade": "medio", "nivel": "medio", "icone": "📈",
                    })
                    break

    # 5. Saldo negativo atual
    if mes_atual["saldo"] < 0:
        alertas.append({
            "texto": f"Saldo negativo este mês: déficit de R$ {abs(mes_atual['saldo']):.2f}. Revise os gastos.",
            "prioridade": "alto", "nivel": "alto", "icone": "⚠",
        })

    # 6. Taxa de poupança baixa
    if taxa_poupanca < 10 and media_entradas > 0:
        alertas.append({
            "texto": f"Taxa de poupança de {max(0, taxa_poupanca):.0f}% — abaixo do ideal mínimo de 10%.",
            "prioridade": "medio", "nivel": "medio", "icone": "💰",
        })

    # 7. Renda abaixo da média
    if media_entradas > 0 and mes_atual["entradas"] < media_entradas * 0.8:
        alertas.append({
            "texto": f"Renda deste mês (R$ {mes_atual['entradas']:.0f}) abaixo da sua média histórica (R$ {media_entradas:.0f}).",
            "prioridade": "medio", "nivel": "medio", "icone": "💸",
        })

    # Positivo (sem alertas críticos)
    if not alertas:
        alertas.append({
            "texto": "Suas finanças estão equilibradas! Continue registrando para análises cada vez mais precisas.",
            "prioridade": "baixo", "nivel": "baixo", "icone": "✅",
        })

    return alertas[:6]


# ── Etapa 15: CSV Import ───────────────────────────────────────────────────────

_CSV_COLS_DATA     = {"data", "date", "dt"}
_CSV_COLS_DESC     = {"descricao", "descrição", "description", "desc", "historico", "histórico", "memo"}
_CSV_COLS_VALOR    = {"valor", "value", "amount", "montante", "quantia"}
_CSV_COLS_TIPO     = {"tipo", "type", "natureza"}
_CSV_COLS_CAT      = {"categoria", "category", "cat", "grupo"}
_CSV_COLS_SUBTIPO  = {"subtipo", "subtype", "modalidade"}

def _detectar_coluna(headers, possiveis):
    for h in headers:
        if h.strip().lower() in possiveis:
            return h
    return None

def _normalizar_tipo(raw):
    raw = (raw or "").strip().lower()
    if raw in {"entrada", "receita", "credit", "credito", "crédito", "in", "e", "+"}:
        return "entrada"
    if raw in {"despesa", "saida", "saída", "gasto", "debit", "debito", "débito", "out", "d", "-"}:
        return "despesa"
    return None

def _normalizar_data(raw):
    raw = raw.strip()
    fmts = ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"]
    for fmt in fmts:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except Exception:
            pass
    return None

def _normalizar_valor(raw):
    try:
        v = raw.strip().replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
        return abs(float(v))
    except Exception:
        return None

def _parsear_csv(content):
    """Parse CSV content and return (linhas_validas, erros)."""
    validas, erros = [], []
    try:
        sample = content[:2048]
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except Exception:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(content), dialect=dialect)
    headers = reader.fieldnames or []
    col_data    = _detectar_coluna(headers, _CSV_COLS_DATA)
    col_desc    = _detectar_coluna(headers, _CSV_COLS_DESC)
    col_valor   = _detectar_coluna(headers, _CSV_COLS_VALOR)
    col_tipo    = _detectar_coluna(headers, _CSV_COLS_TIPO)
    col_cat     = _detectar_coluna(headers, _CSV_COLS_CAT)
    col_subtipo = _detectar_coluna(headers, _CSV_COLS_SUBTIPO)
    if not col_data or not col_valor:
        return [], [{"linha": 0, "erro": "Colunas 'data' e 'valor' são obrigatórias no CSV."}]
    for i, row in enumerate(reader, start=2):
        data_raw  = (row.get(col_data)  or "").strip()
        desc_raw  = (row.get(col_desc)  or "").strip() if col_desc else ""
        valor_raw = (row.get(col_valor) or "").strip()
        tipo_raw  = (row.get(col_tipo)  or "").strip() if col_tipo else ""
        cat_raw   = (row.get(col_cat)   or "").strip() if col_cat else ""
        sub_raw   = (row.get(col_subtipo) or "").strip() if col_subtipo else ""
        data  = _normalizar_data(data_raw)
        valor = _normalizar_valor(valor_raw)
        tipo  = _normalizar_tipo(tipo_raw)
        if not data:
            erros.append({"linha": i, "erro": f"Data inválida: '{data_raw}'"})
            continue
        if valor is None or valor <= 0:
            erros.append({"linha": i, "erro": f"Valor inválido: '{valor_raw}'"})
            continue
        if not desc_raw:
            desc_raw = "Importado via CSV"
        if not tipo:
            tipo = "despesa"
        validas.append({
            "data": data, "descricao": desc_raw, "valor": valor,
            "tipo": tipo, "categoria": cat_raw or "", "subtipo": sub_raw or "",
        })
        if len(validas) >= 500:
            break
    return validas, erros


# ── Helpers de integração fiscal ─────────────────────────────────────────────

CAMPOS_FISCAIS_OBRIGATORIOS = [
    ("razao_social",      "Razão Social / Nome"),
    ("cpf_cnpj",          "CPF / CNPJ"),
    ("email",             "E-mail"),
    ("telefone",          "Telefone"),
    ("endereco",          "Endereço"),
    ("cidade",            "Cidade"),
    ("estado",            "Estado"),
    ("cep",               "CEP"),
    ("regime_tributario", "Regime Tributário"),
]

def validar_config_fiscal(config):
    """Retorna lista de campos obrigatórios ausentes na configuração fiscal."""
    if not config:
        return [label for _, label in CAMPOS_FISCAIS_OBRIGATORIOS]
    return [label for campo, label in CAMPOS_FISCAIS_OBRIGATORIOS
            if not config[campo]]

def montar_payload_fiscal(nota, config_fiscal, integ_config):
    """Monta payload estruturado da nota para futura integração NFS-e."""
    emitente = {}
    if config_fiscal:
        emitente = {
            "razao_social":      config_fiscal["razao_social"] or "",
            "cpf_cnpj":          config_fiscal["cpf_cnpj"] or "",
            "inscricao_municipal": config_fiscal["inscricao_municipal"] or "",
            "email":             config_fiscal["email"] or "",
            "telefone":          config_fiscal["telefone"] or "",
            "endereco":          config_fiscal["endereco"] or "",
            "cidade":            config_fiscal["cidade"] or "",
            "estado":            config_fiscal["estado"] or "",
            "cep":               config_fiscal["cep"] or "",
            "regime_tributario": config_fiscal["regime_tributario"] or "",
        }
    tomador = {
        "nome":      dict(nota)["cliente_nome"],
        "cpf_cnpj":  dict(nota).get("cpf_cnpj") or "",
        "email":     dict(nota).get("email") or "",
        "telefone":  dict(nota).get("telefone") or "",
    }
    servico = {
        "nome":      dict(nota)["servico_nome"],
        "descricao": dict(nota).get("servico_descricao") or "",
        "descricao_nota": dict(nota).get("descricao") or "",
        "valor":     nota["valor"],
    }
    payload = {
        "_versao":        "1.0",
        "_modo":          integ_config["modo_emissao"] if integ_config else "interno",
        "_ambiente":      integ_config["ambiente"] if integ_config else "homologacao",
        "_provedor":      (integ_config["provedor"] or "") if integ_config else "",
        "_municipio_cod": (integ_config["municipio_codigo"] or "") if integ_config else "",
        "numero_nota":    nota["numero_nota"],
        "data_emissao":   nota["data_emissao"],
        "status_interno": nota["status"],
        "emitente":       emitente,
        "tomador":        tomador,
        "servico":        servico,
        "gerado_em":      datetime.now().isoformat(),
    }
    return payload

def registrar_evento_fiscal(conn, nota_id, tipo_evento, status,
                             mensagem=None, protocolo=None,
                             payload=None, resposta=None):
    """Salva um evento de integração fiscal no histórico."""
    conn.execute("""
        INSERT INTO eventos_fiscais
            (nota_id, tipo_evento, status, mensagem, protocolo,
             payload_enviado, resposta_recebida)
        VALUES (?,?,?,?,?,?,?)
    """, (nota_id, tipo_evento, status, mensagem, protocolo,
          json.dumps(payload, ensure_ascii=False) if payload else None,
          json.dumps(resposta, ensure_ascii=False) if resposta else None))

LABEL_INTEGRACAO = {
    "nao_enviada":            ("⬜", "Não enviada",           "integ-nao-enviada"),
    "pronta_para_envio":      ("📋", "Pronta p/ envio",       "integ-pronta"),
    "enviada_homologacao":    ("📤", "Enviada (homolog.)",     "integ-enviada-hom"),
    "enviada_producao":       ("📤", "Enviada (produção)",     "integ-enviada-prod"),
    "autorizada":             ("✅", "Autorizada",             "integ-autorizada"),
    "rejeitada":              ("❌", "Rejeitada",              "integ-rejeitada"),
    "cancelamento_pendente":  ("⏳", "Cancelamento pendente", "integ-canc-pend"),
    "cancelada_externamente": ("🚫", "Cancelada ext.",         "integ-cancelada"),
}

# ── Mapeamento de palavras-chave → categoria/subtipo ─────────────────────────

MAPEAMENTO_CATEGORIAS = [
    (["aluguel", "apto", "apartamento", "casa", "quitinete", "imóvel"], "aluguel",     "fixa"),
    (["luz", "energia", "eletricidade", "cpfl", "cemig", "enel"],        "luz",         "fixa"),
    (["água", "agua", "saneamento", "sabesp", "copasa"],                 "água",        "fixa"),
    (["internet", "wifi", "banda larga", "fibra", "net", "vivo"],        "internet",    "fixa"),
    (["telefone", "celular", "claro", "tim", "oi"],                      "internet",    "fixa"),
    (["mercado", "supermercado", "hortifruti", "feira", "padaria",
      "alimentação", "alimento", "comida", "lanche", "restaurante",
      "ifood", "delivery"],                                              "alimentação", "variável"),
    (["transporte", "uber", "99", "ônibus", "metrô", "metro", "trem",
      "gasolina", "combustível", "combustivel", "etanol", "pedágio"],   "transporte",  "variável"),
    (["compras", "roupa", "vestuário", "calçado", "magazine",
      "americanas", "amazon", "shopee", "shopping"],                    "compras",     "variável"),
    (["lazer", "cinema", "netflix", "spotify", "streaming",
      "viagem", "hotel", "passeio", "entretenimento"],                  "lazer",       "variável"),
    (["salário", "salario", "holerite", "pagamento salário"],            "salário",     None),
    (["renda extra", "freelance", "bico", "consultoria", "serviço"],    "renda extra", None),
    (["investimento", "aplicação", "aplicacao", "renda fixa",
      "tesouro", "fundo", "ações", "acoes", "dividendos"],              "investimento", None),
    (["faturamento", "recebimento cliente", "cobrança"],                 "faturamento", None),
]


def sugerir_categoria(descricao, conn):
    """Retorna dict {categoria, subtipo, valor, confianca} com base na descrição e no histórico."""
    desc_lower = descricao.lower()

    # 1. Padrões aprendidos (maior prioridade)
    chave = descricao[:20].strip().lower()
    padrao = conn.execute(
        "SELECT * FROM padroes_aprendidos WHERE chave_referencia=? ORDER BY frequencia DESC LIMIT 1",
        (chave,)
    ).fetchone()
    if padrao and padrao["categoria_sugerida"]:
        return {
            "categoria": padrao["categoria_sugerida"],
            "subtipo": padrao["subtipo_sugerido"],
            "valor": padrao["valor_medio"] if padrao["valor_medio"] else None,
            "confianca": padrao["nivel_confianca"],
        }

    # 2. Histórico exato por descrição
    historico = conn.execute(
        "SELECT descricao, categoria, subtipo, valor, COUNT(*) as freq FROM lancamentos WHERE descricao LIKE ? GROUP BY categoria ORDER BY freq DESC LIMIT 5",
        (f"%{descricao[:8]}%",)
    ).fetchall()
    if historico:
        h = historico[0]
        freq = h["freq"]
        valor_rec = round(sum(x["valor"] for x in historico) / len(historico), 2)
        confianca = "alta" if freq >= 4 else ("media" if freq >= 2 else "baixa")
        return {"categoria": h["categoria"], "subtipo": h["subtipo"], "valor": valor_rec, "confianca": confianca}

    # 3. Mapeamento de palavras-chave
    for palavras, categoria, subtipo in MAPEAMENTO_CATEGORIAS:
        for palavra in palavras:
            if palavra in desc_lower:
                hist_cat = conn.execute(
                    "SELECT AVG(valor) as media FROM lancamentos WHERE categoria=? AND descricao LIKE ? LIMIT 10",
                    (categoria, f"%{palavra}%")
                ).fetchone()
                valor_rec = round(hist_cat["media"], 2) if hist_cat and hist_cat["media"] else None
                return {"categoria": categoria, "subtipo": subtipo, "valor": valor_rec, "confianca": "media"}

    return None


def calcular_acoes_rapidas(conn):
    """Retorna lista de ações rápidas contextuais baseadas no estado atual do sistema."""
    acoes = []

    # Faturamentos pagos sem lancamento vinculado
    fats_pagos_sem_lanc = conn.execute("""
        SELECT f.id, f.valor, c.nome as cliente_nome, s.nome as servico_nome
        FROM faturamentos f
        JOIN clientes c ON c.id = f.cliente_id
        JOIN servicos s ON s.id = f.servico_id
        WHERE f.status='pago' AND f.lancamento_id IS NULL
        LIMIT 3
    """).fetchall()
    for f in fats_pagos_sem_lanc:
        acoes.append({
            "icone": "💰",
            "titulo": "Registrar entrada de faturamento",
            "descricao": f"{f['cliente_nome']} — R$ {f['valor']:.2f}",
            "url": f"/faturamentos/gerar-entrada/{f['id']}",
            "cor": "acao-verde",
        })

    # Faturamentos elegíveis para nota (pagos, sem nota)
    fats_sem_nota = conn.execute("""
        SELECT f.id, c.nome as cliente_nome, f.valor
        FROM faturamentos f
        JOIN clientes c ON c.id = f.cliente_id
        WHERE f.status='pago'
          AND f.id NOT IN (SELECT faturamento_id FROM notas_fiscais WHERE faturamento_id IS NOT NULL)
        LIMIT 2
    """).fetchall()
    for f in fats_sem_nota:
        acoes.append({
            "icone": "🧾",
            "titulo": "Gerar nota fiscal",
            "descricao": f"{f['cliente_nome']} — R$ {f['valor']:.2f}",
            "url": f"/notas/gerar/{f['id']}",
            "cor": "acao-azul",
        })

    # Notas prontas para integração
    notas_prontas = conn.execute(
        "SELECT COUNT(*) as qtd FROM notas_fiscais WHERE status='emitida' AND status_integracao='pronta_para_envio'"
    ).fetchone()
    if notas_prontas and notas_prontas["qtd"] > 0:
        acoes.append({
            "icone": "📤",
            "titulo": "Notas prontas para integração",
            "descricao": f"{notas_prontas['qtd']} nota(s) aguardando",
            "url": "/status-fiscal",
            "cor": "acao-roxo",
        })

    # Recorrentes pendentes para o período
    hoje_str = date.today().isoformat()
    rec_pendentes = conn.execute(
        "SELECT COUNT(*) as qtd FROM lancamentos_recorrentes WHERE ativo=1 AND proxima_data <= ?",
        (hoje_str,)
    ).fetchone()
    if rec_pendentes and rec_pendentes["qtd"] > 0:
        acoes.append({
            "icone": "🔄",
            "titulo": "Gerar lançamentos recorrentes",
            "descricao": f"{rec_pendentes['qtd']} recorrente(s) no vencimento",
            "url": "/recorrentes/gerar",
            "cor": "acao-amarelo",
        })

    # Metas com saldo positivo disponível
    saldo_geral = conn.execute(
        "SELECT COALESCE(SUM(CASE WHEN tipo='entrada' THEN valor ELSE -valor END),0) as s FROM lancamentos"
    ).fetchone()
    metas_em_andamento = conn.execute(
        "SELECT COUNT(*) as qtd FROM metas WHERE valor_atual < valor_objetivo"
    ).fetchone()
    if saldo_geral and saldo_geral["s"] > 0 and metas_em_andamento and metas_em_andamento["qtd"] > 0:
        acoes.append({
            "icone": "🎯",
            "titulo": "Atualizar progresso de meta",
            "descricao": f"Saldo disponível: R$ {saldo_geral['s']:.2f}",
            "url": "/#sec-metas",
            "cor": "acao-verde",
        })

    # Backup rápido
    acoes.append({
        "icone": "💾",
        "titulo": "Baixar backup completo",
        "descricao": "Código + banco de dados",
        "url": "/backup/completo",
        "cor": "acao-cinza",
    })

    return acoes[:6]


def calcular_proxima_acao(conn):
    """Retorna a próxima melhor ação sugerida ao usuário."""
    hoje_str = date.today().isoformat()
    hoje_dt  = date.today()
    mes_ini  = hoje_dt.replace(day=1).isoformat()

    # Sem lançamentos este mês
    lancs_mes = conn.execute(
        "SELECT COUNT(*) as qtd FROM lancamentos WHERE data >= ?", (mes_ini,)
    ).fetchone()
    if lancs_mes and lancs_mes["qtd"] == 0:
        return {
            "frase": "Registre suas despesas fixas do mês para começar o controle financeiro.",
            "url": "/#form-despesa",
            "label": "Registrar despesa",
        }

    # Faturamento pago sem entrada
    fat_pendente = conn.execute(
        "SELECT COUNT(*) as qtd FROM faturamentos WHERE status='pago' AND lancamento_id IS NULL"
    ).fetchone()
    if fat_pendente and fat_pendente["qtd"] > 0:
        return {
            "frase": f"Você tem {fat_pendente['qtd']} faturamento(s) pago(s) ainda não registrado(s) como entrada.",
            "url": "/gestao#sec-faturamentos",
            "label": "Ver faturamentos",
        }

    # Nota de rascunho pendente
    nota_rasc = conn.execute(
        "SELECT COUNT(*) as qtd FROM notas_fiscais WHERE status='rascunho'"
    ).fetchone()
    if nota_rasc and nota_rasc["qtd"] > 0:
        return {
            "frase": f"Você tem {nota_rasc['qtd']} nota(s) em rascunho aguardando emissão.",
            "url": "/notas?status=rascunho",
            "label": "Ver notas",
        }

    # Configuração fiscal incompleta
    config_fiscal = conn.execute("SELECT * FROM configuracao_fiscal ORDER BY id DESC LIMIT 1").fetchone()
    if not config_fiscal or not config_fiscal["razao_social"]:
        return {
            "frase": "Sua configuração fiscal ainda não foi preenchida. Isso é necessário para emitir notas.",
            "url": "/configuracao-fiscal",
            "label": "Configurar dados fiscais",
        }

    # Recorrentes com vencimento
    rec = conn.execute(
        "SELECT COUNT(*) as qtd FROM lancamentos_recorrentes WHERE ativo=1 AND proxima_data <= ?",
        (hoje_str,)
    ).fetchone()
    if rec and rec["qtd"] > 0:
        return {
            "frase": f"Há {rec['qtd']} lançamento(s) recorrente(s) no vencimento para gerar.",
            "url": "/recorrentes",
            "label": "Ver recorrentes",
        }

    # Meta próxima do prazo
    meta_urgente = conn.execute(
        "SELECT nome, prazo FROM metas WHERE valor_atual < valor_objetivo AND prazo IS NOT NULL AND prazo <= ? ORDER BY prazo ASC LIMIT 1",
        ((hoje_dt + timedelta(days=30)).isoformat(),)
    ).fetchone()
    if meta_urgente:
        return {
            "frase": f"A meta '{meta_urgente['nome']}' vence em breve. Atualize o progresso.",
            "url": "/#sec-metas",
            "label": "Ver metas",
        }

    # Categoria que mais cresceu
    return {
        "frase": "Revise a categoria com maior gasto no período e identifique possíveis reduções.",
        "url": "/inteligencia",
        "label": "Ver inteligência financeira",
    }

# ── Favicon ───────────────────────────────────────────────────────────────────

@app.route("/favicon.ico")
def favicon():
    ico_path = os.path.join(app.root_path, "static", "favicon.ico")
    if os.path.exists(ico_path):
        return send_file(ico_path, mimetype="image/x-icon")
    return "", 204

# ── Rotas principais ──────────────────────────────────────────────────────────

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        tipo      = request.form.get("tipo")
        data      = request.form.get("data")
        descricao = request.form.get("descricao")
        categoria = request.form.get("categoria")
        valor     = float(request.form.get("valor", 0))
        subtipo   = request.form.get("subtipo", "")
        conn = get_db()
        conn.execute(
            "INSERT INTO lancamentos (tipo, data, descricao, categoria, valor, subtipo) VALUES (?, ?, ?, ?, ?, ?)",
            (tipo, data, descricao, categoria, valor, subtipo)
        )
        conn.commit()
        conn.close()
        periodo_qs = request.form.get("periodo_ativo", "todos")
        label = "Entrada" if tipo == "entrada" else "Despesa"
        flash(f"{label} registrada com sucesso.", "success")
        return redirect(url_for("index", periodo=periodo_qs))

    periodo      = request.args.get("periodo", "todos")
    filtro_tipo  = request.args.get("tipo", "")
    filtro_cat   = request.args.get("categoria", "")
    filtro_sub   = request.args.get("subtipo", "")
    filtro_metas = request.args.get("metas", "")

    data_ini, data_fim = get_periodo_dates(periodo)

    conn = get_db()

    where, params = [], []
    if data_ini and data_fim:
        where.append("data BETWEEN ? AND ?")
        params += [data_ini, data_fim]
    if filtro_tipo:
        where.append("tipo = ?")
        params.append(filtro_tipo)
    if filtro_cat:
        where.append("categoria = ?")
        params.append(filtro_cat)
    if filtro_sub:
        where.append("subtipo = ?")
        params.append(filtro_sub)

    sql = "SELECT * FROM lancamentos"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY data DESC, id DESC"
    lancamentos = conn.execute(sql, params).fetchall()

    categorias = [r[0] for r in conn.execute(
        "SELECT DISTINCT categoria FROM lancamentos WHERE categoria IS NOT NULL AND categoria != '' ORDER BY categoria"
    ).fetchall()]

    total_entradas  = sum(l["valor"] for l in lancamentos if l["tipo"] == "entrada")
    total_saidas    = sum(l["valor"] for l in lancamentos if l["tipo"] == "despesa")
    saldo           = total_entradas - total_saidas
    quantidade      = len(lancamentos)
    total_fixas     = sum(l["valor"] for l in lancamentos if l["tipo"] == "despesa" and l["subtipo"] == "fixa")
    total_variaveis = sum(l["valor"] for l in lancamentos if l["tipo"] == "despesa" and l["subtipo"] == "variável")

    projecao, alertas, frases = calcular_inteligencia(
        total_entradas, total_saidas, saldo, total_fixas, total_variaveis
    )
    metas_todas, reserva, patrimonial, frases_pat = calcular_patrimonio(conn)

    # ── Dados extras para o dashboard ────────────────────────────────────────
    hoje_dt = date.today()
    mes_ini = hoje_dt.replace(day=1).isoformat()
    mes_fim = hoje_dt.isoformat()

    # Faturamento do mês
    fats_mes = conn.execute(
        "SELECT * FROM faturamentos WHERE data BETWEEN ? AND ?", (mes_ini, mes_fim)
    ).fetchall()
    fat_mes_total    = sum(f["valor"] for f in fats_mes)
    fat_mes_pendente = sum(f["valor"] for f in fats_mes if f["status"] == "pendente")

    # Notas do mês
    notas_mes = conn.execute(
        "SELECT * FROM notas_fiscais WHERE data_emissao BETWEEN ? AND ?", (mes_ini, mes_fim)
    ).fetchall()
    notas_emitidas_mes  = len([n for n in notas_mes if n["status"] == "emitida"])
    notas_rascunhos_mes = len([n for n in notas_mes if n["status"] == "rascunho"])

    # Metas em andamento
    metas_andamento_qtd = len([m for m in metas_todas if m["valor_atual"] < m["valor_objetivo"]])

    # Últimas movimentações (5 lançamentos + 3 faturamentos + 3 notas)
    ultimos_lanc = conn.execute(
        "SELECT * FROM lancamentos ORDER BY data DESC, id DESC LIMIT 5"
    ).fetchall()
    ultimos_fat = conn.execute("""
        SELECT f.*, c.nome as cliente_nome, s.nome as servico_nome
        FROM faturamentos f
        JOIN clientes c ON c.id = f.cliente_id
        JOIN servicos s ON s.id = f.servico_id
        ORDER BY f.data DESC, f.id DESC LIMIT 3
    """).fetchall()
    ultimas_notas = conn.execute("""
        SELECT n.*, c.nome as cliente_nome, s.nome as servico_nome
        FROM notas_fiscais n
        JOIN clientes c ON c.id = n.cliente_id
        JOIN servicos s ON s.id = n.servico_id
        ORDER BY n.id DESC LIMIT 3
    """).fetchall()

    # Pendências
    pend_fat = conn.execute("""
        SELECT f.*, c.nome as cliente_nome, s.nome as servico_nome
        FROM faturamentos f
        JOIN clientes c ON c.id = f.cliente_id
        JOIN servicos s ON s.id = f.servico_id
        WHERE f.status = 'pendente'
        ORDER BY f.data ASC LIMIT 5
    """).fetchall()
    pend_notas = conn.execute("""
        SELECT n.*, c.nome as cliente_nome
        FROM notas_fiscais n
        JOIN clientes c ON c.id = n.cliente_id
        WHERE n.status = 'rascunho'
        ORDER BY n.id ASC LIMIT 5
    """).fetchall()
    pend_metas = [m for m in metas_todas
                  if m["valor_atual"] < m["valor_objetivo"]
                  and m["prazo"] and m["prazo"] < hoje_dt.isoformat()][:5]

    # Configuração fiscal (para mostrar status no dashboard)
    config_fiscal = conn.execute(
        "SELECT * FROM configuracao_fiscal ORDER BY id DESC LIMIT 1"
    ).fetchone()

    # Automação: Ações Rápidas e Próxima Melhor Ação
    acoes_rapidas  = calcular_acoes_rapidas(conn)
    proxima_acao   = calcular_proxima_acao(conn)

    # Recorrentes com vencimento
    hoje_str_rec = hoje_dt.isoformat()
    rec_pendentes_qtd = conn.execute(
        "SELECT COUNT(*) as qtd FROM lancamentos_recorrentes WHERE ativo=1 AND proxima_data <= ?",
        (hoje_str_rec,)
    ).fetchone()["qtd"]

    # ── Dados para Dashboard Visual (Etapa 18) ────────────────────────────────
    dados_mensais_vis = _buscar_dados_mensais(conn, 6)

    # Categorias de gasto (período todo)
    cat_rows = conn.execute("""
        SELECT categoria, SUM(valor) as total
        FROM lancamentos
        WHERE tipo='despesa' AND categoria IS NOT NULL AND categoria != ''
        GROUP BY categoria
        ORDER BY total DESC
        LIMIT 8
    """).fetchall()
    total_cat = sum(r["total"] for r in cat_rows) or 1
    categorias_visuais = [
        {"nome": r["categoria"], "total": r["total"],
         "pct": round(r["total"] / total_cat * 100, 1)}
        for r in cat_rows
    ]

    # Stats de faturamentos (total)
    fat_stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status='pago'     THEN 1 ELSE 0 END) as pagos,
            SUM(CASE WHEN status='pendente' THEN 1 ELSE 0 END) as pendentes,
            SUM(CASE WHEN status='pago'     THEN valor ELSE 0 END) as valor_pago,
            SUM(CASE WHEN status='pendente' THEN valor ELSE 0 END) as valor_pendente
        FROM faturamentos
    """).fetchone()

    # Stats de notas fiscais (total)
    nf_stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status='emitida'  THEN 1 ELSE 0 END) as emitidas,
            SUM(CASE WHEN status='rascunho' THEN 1 ELSE 0 END) as rascunhos,
            SUM(CASE WHEN status='cancelada' THEN 1 ELSE 0 END) as canceladas,
            SUM(CASE WHEN status='emitida'  THEN valor ELSE 0 END) as valor_emitido
        FROM notas_fiscais
    """).fetchone()

    conn.close()

    if filtro_metas == "concluidas":
        metas = [m for m in metas_todas if m["valor_atual"] >= m["valor_objetivo"] and m["valor_objetivo"] > 0]
    elif filtro_metas == "andamento":
        metas = [m for m in metas_todas if m["valor_atual"] < m["valor_objetivo"]]
    else:
        metas = metas_todas

    hoje = hoje_dt.isoformat()

    return render_template("index.html",
        lancamentos=lancamentos,
        total_entradas=total_entradas,
        total_saidas=total_saidas,
        saldo=saldo,
        quantidade=quantidade,
        total_fixas=total_fixas,
        total_variaveis=total_variaveis,
        projecao=projecao,
        alertas=alertas,
        frases=frases,
        metas=metas,
        metas_todas=metas_todas,
        reserva=reserva,
        patrimonial=patrimonial,
        frases_pat=frases_pat,
        periodo=periodo,
        hoje=hoje,
        categorias=categorias,
        filtro_tipo=filtro_tipo,
        filtro_cat=filtro_cat,
        filtro_sub=filtro_sub,
        filtro_metas=filtro_metas,
        fat_mes_total=fat_mes_total,
        fat_mes_pendente=fat_mes_pendente,
        notas_emitidas_mes=notas_emitidas_mes,
        notas_rascunhos_mes=notas_rascunhos_mes,
        metas_andamento_qtd=metas_andamento_qtd,
        ultimos_lanc=ultimos_lanc,
        ultimos_fat=ultimos_fat,
        ultimas_notas=ultimas_notas,
        pend_fat=pend_fat,
        pend_notas=pend_notas,
        pend_metas=pend_metas,
        config_fiscal=config_fiscal,
        acoes_rapidas=acoes_rapidas,
        proxima_acao=proxima_acao,
        rec_pendentes_qtd=rec_pendentes_qtd,
        dados_mensais_vis=dados_mensais_vis,
        categorias_visuais=categorias_visuais,
        fat_stats=fat_stats,
        nf_stats=nf_stats,
    )

@app.route("/excluir/<int:id>")
def excluir(id):
    periodo = request.args.get("periodo", "todos")
    conn = get_db()
    conn.execute("DELETE FROM lancamentos WHERE id = ?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", periodo=periodo))

# ── Rotas de metas ────────────────────────────────────────────────────────────

@app.route("/metas/adicionar", methods=["POST"])
def meta_adicionar():
    nome           = request.form.get("nome", "").strip()
    valor_objetivo = float(request.form.get("valor_objetivo", 0) or 0)
    valor_atual    = float(request.form.get("valor_atual", 0) or 0)
    prazo          = request.form.get("prazo", "").strip() or None
    descricao      = request.form.get("descricao", "").strip() or None
    tipo           = request.form.get("tipo_meta", "meta")
    periodo        = request.form.get("periodo_ativo", "todos")

    if nome and valor_objetivo > 0:
        conn = get_db()
        conn.execute(
            "INSERT INTO metas (nome, valor_objetivo, valor_atual, prazo, descricao, tipo) VALUES (?, ?, ?, ?, ?, ?)",
            (nome, valor_objetivo, valor_atual, prazo, descricao, tipo)
        )
        conn.commit()
        conn.close()
    return redirect(url_for("index", periodo=periodo) + "#sec-metas")

@app.route("/metas/excluir/<int:id>")
def meta_excluir(id):
    periodo = request.args.get("periodo", "todos")
    conn = get_db()
    conn.execute("DELETE FROM metas WHERE id = ?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("index", periodo=periodo) + "#sec-metas")

@app.route("/metas/progresso/<int:id>", methods=["POST"])
def meta_progresso(id):
    valor_atual = float(request.form.get("valor_atual", 0) or 0)
    periodo     = request.form.get("periodo_ativo", "todos")
    conn = get_db()
    conn.execute("UPDATE metas SET valor_atual = ? WHERE id = ?", (valor_atual, id))
    conn.commit()
    conn.close()
    return redirect(url_for("index", periodo=periodo) + "#sec-metas")

@app.route("/metas/editar/<int:id>", methods=["GET", "POST"])
def meta_editar(id):
    conn = get_db()
    if request.method == "POST":
        nome           = request.form.get("nome", "").strip()
        valor_objetivo = float(request.form.get("valor_objetivo", 0) or 0)
        valor_atual    = float(request.form.get("valor_atual", 0) or 0)
        prazo          = request.form.get("prazo", "").strip() or None
        descricao      = request.form.get("descricao", "").strip() or None
        tipo           = request.form.get("tipo_meta", "meta")
        periodo        = request.form.get("periodo_ativo", "todos")
        conn.execute(
            "UPDATE metas SET nome=?, valor_objetivo=?, valor_atual=?, prazo=?, descricao=?, tipo=? WHERE id=?",
            (nome, valor_objetivo, valor_atual, prazo, descricao, tipo, id)
        )
        conn.commit()
        conn.close()
        return redirect(url_for("index", periodo=periodo) + "#sec-metas")

    meta    = conn.execute("SELECT * FROM metas WHERE id = ?", (id,)).fetchone()
    conn.close()
    periodo = request.args.get("periodo", "todos")
    return render_template("editar_meta.html", meta=meta, periodo=periodo)

# ── Contador Inteligente ──────────────────────────────────────────────────────

def simular_fiscal(renda_media, faturamento, despesas_trabalho):
    """Simulação fiscal simplificada. Apenas educativa."""

    # ── Pessoa Física ──
    # INSS simplificado (alíquotas progressivas 2024)
    if renda_media <= 1412.00:
        inss_pf = renda_media * 0.075
    elif renda_media <= 2666.68:
        inss_pf = renda_media * 0.09
    elif renda_media <= 4000.03:
        inss_pf = renda_media * 0.12
    elif renda_media <= 7786.02:
        inss_pf = renda_media * 0.14
    else:
        inss_pf = 7786.02 * 0.14  # teto INSS

    base_irpf = max(0.0, renda_media - inss_pf - 528.00)  # dedução simplificada
    if base_irpf <= 2259.20:
        irpf = 0.0
    elif base_irpf <= 2826.65:
        irpf = base_irpf * 0.075 - 169.44
    elif base_irpf <= 3751.05:
        irpf = base_irpf * 0.15 - 381.44
    elif base_irpf <= 4664.68:
        irpf = base_irpf * 0.225 - 662.77
    else:
        irpf = base_irpf * 0.275 - 896.00
    irpf = max(0.0, irpf)

    custo_pf   = inss_pf + irpf
    liquido_pf = renda_media - custo_pf

    # ── MEI ──
    MEI_LIMITE_MES = 6750.00
    MEI_DAS        = 75.90   # valor fixo mensal (2024, prestador de serviços)
    mei_valido = faturamento <= MEI_LIMITE_MES

    if mei_valido:
        custo_mei   = MEI_DAS
        liquido_mei = faturamento - despesas_trabalho - custo_mei
        mei_detalhe = f"DAS fixo: R$ {MEI_DAS:.2f}/mês"
    else:
        custo_mei   = 0.0
        liquido_mei = None
        mei_detalhe = f"Faturamento excede o limite MEI (R$ 6.750/mês)"

    # ── PJ – Simples Nacional Anexo III (serviços) ──
    fat_anual = faturamento * 12
    if fat_anual <= 180_000:
        aliq = 0.060
    elif fat_anual <= 360_000:
        aliq = 0.112
    elif fat_anual <= 720_000:
        aliq = 0.135
    else:
        aliq = 0.160

    prolabore  = faturamento * 0.28
    inss_pj    = min(prolabore * 0.11, 7786.02 * 0.14)
    simples    = faturamento * aliq
    custo_pj   = simples + inss_pj
    liquido_pj = faturamento - despesas_trabalho - custo_pj

    resultados = [
        {
            "nome": "Pessoa Física",
            "sigla": "PF",
            "custo": custo_pf,
            "liquido": liquido_pf,
            "detalhe": f"INSS: R$ {inss_pf:.2f}  |  IRPF: R$ {irpf:.2f}",
            "valido": True,
        },
        {
            "nome": "MEI",
            "sigla": "MEI",
            "custo": custo_mei,
            "liquido": liquido_mei,
            "detalhe": mei_detalhe,
            "valido": mei_valido,
        },
        {
            "nome": "PJ – Simples Nacional",
            "sigla": "PJ",
            "custo": custo_pj,
            "liquido": liquido_pj,
            "detalhe": f"Simples {aliq*100:.1f}%: R$ {simples:.2f}  |  INSS pró-labore: R$ {inss_pj:.2f}",
            "valido": True,
        },
    ]

    # Melhor cenário = maior líquido entre os válidos
    validos = [(r["liquido"], r["nome"]) for r in resultados if r["valido"] and r["liquido"] is not None]
    melhor_nome = max(validos, key=lambda x: x[0])[1] if validos else None
    for r in resultados:
        r["melhor"] = r["nome"] == melhor_nome and r["valido"]

    return resultados, melhor_nome


def gerar_sugestoes_fiscal(tipo_atuacao, renda_media, faturamento, despesas_trabalho, resultados, melhor_nome):
    sugestoes = []
    pf  = next((r for r in resultados if r["sigla"] == "PF"),  None)
    mei = next((r for r in resultados if r["sigla"] == "MEI"), None)
    pj  = next((r for r in resultados if r["sigla"] == "PJ"),  None)

    if faturamento > 6750:
        sugestoes.append("Seu faturamento supera o limite do MEI — avaliar abertura de PJ pode ser mais vantajoso.")

    pct_desp = (despesas_trabalho / faturamento * 100) if faturamento > 0 else 0
    if pct_desp > 30:
        sugestoes.append(f"Suas despesas profissionais representam {pct_desp:.0f}% do faturamento — registre tudo para aproveitá-las na base tributável.")

    tipo_map = {"pf": "Pessoa Física", "mei": "MEI", "pj": "PJ – Simples Nacional"}
    tipo_atual_nome = tipo_map.get(tipo_atuacao, tipo_atuacao)
    if melhor_nome and melhor_nome != tipo_atual_nome:
        sugestoes.append(f"Seu faturamento pode justificar um modelo mais vantajoso do que '{tipo_atual_nome}'.")

    if pj and pf and pj["liquido"] is not None and pf["liquido"] is not None:
        dif = pj["liquido"] - pf["liquido"]
        if dif > 100:
            sugestoes.append(f"Como PJ, o líquido estimado seria R$ {dif:.2f}/mês maior do que como Pessoa Física.")

    if renda_media > 4000:
        sugestoes.append("Atenção ao crescimento da renda — pode ser momento de revisar seu enquadramento fiscal.")

    sugestoes.append("Mantenha seus registros organizados para facilitar decisões fiscais futuras.")
    return sugestoes


@app.route("/contador", methods=["GET", "POST"])
def contador():
    conn = get_db()

    if request.method == "POST":
        tipo_atuacao      = request.form.get("tipo_atuacao", "pf")
        renda_media       = float(request.form.get("renda_media", 0) or 0)
        faturamento       = float(request.form.get("faturamento_estimado", 0) or 0)
        despesas_trabalho = float(request.form.get("despesas_trabalho", 0) or 0)
        observacoes       = request.form.get("observacoes", "").strip() or None

        conn.execute("DELETE FROM perfil_fiscal")
        conn.execute(
            "INSERT INTO perfil_fiscal (tipo_atuacao, renda_media, faturamento_estimado, despesas_trabalho, observacoes) VALUES (?, ?, ?, ?, ?)",
            (tipo_atuacao, renda_media, faturamento, despesas_trabalho, observacoes)
        )
        conn.commit()
        conn.close()
        return redirect(url_for("contador") + "?simulado=1")

    perfil    = conn.execute("SELECT * FROM perfil_fiscal ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()

    simulado    = request.args.get("simulado") == "1"
    resultados  = None
    melhor_nome = None
    sugestoes   = []
    resumo      = None

    if perfil and simulado:
        resultados, melhor_nome = simular_fiscal(
            perfil["renda_media"], perfil["faturamento_estimado"], perfil["despesas_trabalho"]
        )
        sugestoes = gerar_sugestoes_fiscal(
            perfil["tipo_atuacao"], perfil["renda_media"],
            perfil["faturamento_estimado"], perfil["despesas_trabalho"],
            resultados, melhor_nome
        )
        melhor_r = next((r for r in resultados if r["nome"] == melhor_nome), None)
        resumo = {
            "renda_media":       perfil["renda_media"],
            "faturamento":       perfil["faturamento_estimado"],
            "despesas_trabalho": perfil["despesas_trabalho"],
            "sobra_bruta":       perfil["faturamento_estimado"] - perfil["despesas_trabalho"],
            "melhor_cenario":    melhor_nome or "—",
            "liquido_melhor":    melhor_r["liquido"] if melhor_r and melhor_r["liquido"] is not None else 0,
        }

    return render_template("contador.html",
        perfil=perfil,
        simulado=simulado,
        resultados=resultados,
        melhor_nome=melhor_nome,
        sugestoes=sugestoes,
        resumo=resumo,
    )

# ── Gestão: Clientes, Serviços, Faturamento ───────────────────────────────────

@app.route("/gestao")
def gestao():
    fat_cliente = request.args.get("fat_cliente", "")
    fat_servico = request.args.get("fat_servico", "")
    fat_status  = request.args.get("fat_status", "")
    fat_periodo = request.args.get("fat_periodo", "todos")

    fat_ini, fat_fim = get_periodo_dates(fat_periodo)

    conn = get_db()
    clientes = conn.execute("SELECT * FROM clientes ORDER BY nome").fetchall()
    servicos = conn.execute("SELECT * FROM servicos ORDER BY nome").fetchall()

    fat_where, fat_params = [], []
    if fat_cliente:
        fat_where.append("f.cliente_id = ?")
        fat_params.append(fat_cliente)
    if fat_servico:
        fat_where.append("f.servico_id = ?")
        fat_params.append(fat_servico)
    if fat_status:
        fat_where.append("f.status = ?")
        fat_params.append(fat_status)
    if fat_ini and fat_fim:
        fat_where.append("f.data BETWEEN ? AND ?")
        fat_params += [fat_ini, fat_fim]

    fat_sql = """
        SELECT f.*, c.nome as cliente_nome, s.nome as servico_nome
        FROM faturamentos f
        JOIN clientes c ON c.id = f.cliente_id
        JOIN servicos s ON s.id = f.servico_id
    """
    if fat_where:
        fat_sql += " WHERE " + " AND ".join(fat_where)
    fat_sql += " ORDER BY f.data DESC, f.id DESC"

    faturamentos   = conn.execute(fat_sql, fat_params).fetchall()
    total_pendente = sum(f["valor"] for f in faturamentos if f["status"] == "pendente")
    total_pago     = sum(f["valor"] for f in faturamentos if f["status"] == "pago")

    rows_nf = conn.execute("SELECT id, faturamento_id FROM notas_fiscais WHERE faturamento_id IS NOT NULL").fetchall()
    fat_com_nota = {r["faturamento_id"]: r["id"] for r in rows_nf}
    conn.close()

    return render_template("gestao.html",
        clientes=clientes,
        servicos=servicos,
        faturamentos=faturamentos,
        total_pendente=total_pendente,
        total_pago=total_pago,
        hoje=date.today().isoformat(),
        fat_com_nota=fat_com_nota,
        fat_cliente=fat_cliente,
        fat_servico=fat_servico,
        fat_status=fat_status,
        fat_periodo=fat_periodo,
    )

# ── Clientes ──────────────────────────────────────────────────────────────────

@app.route("/clientes/adicionar", methods=["POST"])
def cliente_adicionar():
    nome       = request.form.get("nome", "").strip()
    tipo       = request.form.get("tipo", "pf")
    cpf_cnpj   = request.form.get("cpf_cnpj", "").strip() or None
    email      = request.form.get("email", "").strip() or None
    telefone   = request.form.get("telefone", "").strip() or None
    observacoes = request.form.get("observacoes", "").strip() or None
    if nome:
        conn = get_db()
        conn.execute(
            "INSERT INTO clientes (nome, tipo, cpf_cnpj, email, telefone, observacoes) VALUES (?,?,?,?,?,?)",
            (nome, tipo, cpf_cnpj, email, telefone, observacoes)
        )
        conn.commit()
        conn.close()
        flash("Cliente adicionado com sucesso.", "success")
    return redirect(url_for("gestao") + "#sec-clientes")

@app.route("/clientes/editar/<int:id>", methods=["GET", "POST"])
def cliente_editar(id):
    conn = get_db()
    if request.method == "POST":
        nome       = request.form.get("nome", "").strip()
        tipo       = request.form.get("tipo", "pf")
        cpf_cnpj   = request.form.get("cpf_cnpj", "").strip() or None
        email      = request.form.get("email", "").strip() or None
        telefone   = request.form.get("telefone", "").strip() or None
        observacoes = request.form.get("observacoes", "").strip() or None
        conn.execute(
            "UPDATE clientes SET nome=?, tipo=?, cpf_cnpj=?, email=?, telefone=?, observacoes=? WHERE id=?",
            (nome, tipo, cpf_cnpj, email, telefone, observacoes, id)
        )
        conn.commit()
        conn.close()
        return redirect(url_for("gestao") + "#sec-clientes")
    cliente = conn.execute("SELECT * FROM clientes WHERE id=?", (id,)).fetchone()
    conn.close()
    return render_template("editar_cliente.html", cliente=cliente)

@app.route("/clientes/excluir/<int:id>")
def cliente_excluir(id):
    conn = get_db()
    conn.execute("DELETE FROM clientes WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("gestao") + "#sec-clientes")

# ── Serviços ──────────────────────────────────────────────────────────────────

@app.route("/servicos/adicionar", methods=["POST"])
def servico_adicionar():
    nome        = request.form.get("nome", "").strip()
    descricao   = request.form.get("descricao", "").strip() or None
    valor_padrao = float(request.form.get("valor_padrao", 0) or 0)
    if nome:
        conn = get_db()
        conn.execute(
            "INSERT INTO servicos (nome, descricao, valor_padrao) VALUES (?,?,?)",
            (nome, descricao, valor_padrao)
        )
        conn.commit()
        conn.close()
        flash("Serviço adicionado com sucesso.", "success")
    return redirect(url_for("gestao") + "#sec-servicos")

@app.route("/servicos/editar/<int:id>", methods=["GET", "POST"])
def servico_editar(id):
    conn = get_db()
    if request.method == "POST":
        nome        = request.form.get("nome", "").strip()
        descricao   = request.form.get("descricao", "").strip() or None
        valor_padrao = float(request.form.get("valor_padrao", 0) or 0)
        conn.execute(
            "UPDATE servicos SET nome=?, descricao=?, valor_padrao=? WHERE id=?",
            (nome, descricao, valor_padrao, id)
        )
        conn.commit()
        conn.close()
        return redirect(url_for("gestao") + "#sec-servicos")
    servico = conn.execute("SELECT * FROM servicos WHERE id=?", (id,)).fetchone()
    conn.close()
    return render_template("editar_servico.html", servico=servico)

@app.route("/servicos/excluir/<int:id>")
def servico_excluir(id):
    conn = get_db()
    conn.execute("DELETE FROM servicos WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("gestao") + "#sec-servicos")

# ── Faturamentos ──────────────────────────────────────────────────────────────

@app.route("/faturamentos/adicionar", methods=["POST"])
def faturamento_adicionar():
    cliente_id = request.form.get("cliente_id")
    servico_id = request.form.get("servico_id")
    descricao  = request.form.get("descricao", "").strip() or None
    valor      = float(request.form.get("valor", 0) or 0)
    data       = request.form.get("data") or date.today().isoformat()
    if cliente_id and servico_id and valor > 0:
        conn = get_db()
        conn.execute(
            "INSERT INTO faturamentos (cliente_id, servico_id, descricao, valor, data, status) VALUES (?,?,?,?,?,'pendente')",
            (cliente_id, servico_id, descricao, valor, data)
        )
        conn.commit()
        conn.close()
        flash("Faturamento registrado com sucesso.", "success")
    return redirect(url_for("gestao") + "#sec-faturamentos")

@app.route("/faturamentos/pagar/<int:id>")
def faturamento_pagar(id):
    conn = get_db()
    fat = conn.execute("""
        SELECT f.*, c.nome as cliente_nome, s.nome as servico_nome
        FROM faturamentos f
        JOIN clientes c ON c.id = f.cliente_id
        JOIN servicos s ON s.id = f.servico_id
        WHERE f.id = ?
    """, (id,)).fetchone()

    if fat and fat["status"] == "pendente":
        conn.execute("UPDATE faturamentos SET status='pago' WHERE id=?", (id,))
        descricao_lanc = f"Faturamento: {fat['cliente_nome']} — {fat['servico_nome']}"
        cur = conn.execute(
            "INSERT INTO lancamentos (tipo, data, descricao, categoria, valor, subtipo) VALUES (?,?,?,?,?,?)",
            ("entrada", fat["data"], descricao_lanc, "faturamento", fat["valor"], None)
        )
        lanc_id = cur.lastrowid
        conn.execute("UPDATE faturamentos SET lancamento_id=? WHERE id=?", (lanc_id, id))
        conn.commit()
        flash("Faturamento marcado como pago. Entrada registrada automaticamente.", "success")
    conn.close()
    return redirect(url_for("gestao") + "#sec-faturamentos")


@app.route("/faturamentos/gerar-entrada/<int:id>")
def faturamento_gerar_entrada(id):
    conn = get_db()
    fat = conn.execute("""
        SELECT f.*, c.nome as cliente_nome, s.nome as servico_nome
        FROM faturamentos f
        JOIN clientes c ON c.id = f.cliente_id
        JOIN servicos s ON s.id = f.servico_id
        WHERE f.id=?
    """, (id,)).fetchone()
    if fat and fat["lancamento_id"] is None and fat["status"] == "pago":
        descricao_lanc = f"Faturamento: {fat['cliente_nome']} — {fat['servico_nome']}"
        cur = conn.execute(
            "INSERT INTO lancamentos (tipo, data, descricao, categoria, valor, subtipo) VALUES (?,?,?,?,?,?)",
            ("entrada", fat["data"], descricao_lanc, "faturamento", fat["valor"], None)
        )
        conn.execute("UPDATE faturamentos SET lancamento_id=? WHERE id=?", (cur.lastrowid, id))
        conn.commit()
    conn.close()
    return redirect(url_for("index"))

@app.route("/faturamentos/excluir/<int:id>")
def faturamento_excluir(id):
    conn = get_db()
    conn.execute("DELETE FROM faturamentos WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("gestao") + "#sec-faturamentos")

# ── Notas Fiscais ─────────────────────────────────────────────────────────────

def _nota_join():
    return """
        SELECT n.*, c.nome as cliente_nome, c.tipo as cliente_tipo,
               c.cpf_cnpj, c.email, c.telefone,
               s.nome as servico_nome, s.descricao as servico_descricao
        FROM notas_fiscais n
        JOIN clientes c ON c.id = n.cliente_id
        JOIN servicos s ON s.id = n.servico_id
    """

@app.route("/notas")
def notas():
    f_status  = request.args.get("status", "")
    f_cliente = request.args.get("cliente_id", "")
    f_servico = request.args.get("servico_id", "")
    f_periodo = request.args.get("periodo", "todos")

    nf_ini, nf_fim = get_periodo_dates(f_periodo)

    conn = get_db()
    clientes = conn.execute("SELECT * FROM clientes ORDER BY nome").fetchall()
    servicos = conn.execute("SELECT * FROM servicos ORDER BY nome").fetchall()

    nf_where, nf_params = [], []
    if f_status:
        nf_where.append("n.status = ?")
        nf_params.append(f_status)
    if f_cliente:
        nf_where.append("n.cliente_id = ?")
        nf_params.append(f_cliente)
    if f_servico:
        nf_where.append("n.servico_id = ?")
        nf_params.append(f_servico)
    if nf_ini and nf_fim:
        nf_where.append("n.data_emissao BETWEEN ? AND ?")
        nf_params += [nf_ini, nf_fim]

    sql = _nota_join()
    if nf_where:
        sql += " WHERE " + " AND ".join(nf_where)
    sql += " ORDER BY n.id DESC"
    notas_list = conn.execute(sql, nf_params).fetchall()
    conn.close()

    emitidas   = [n for n in notas_list if n["status"] == "emitida"]
    rascunhos  = [n for n in notas_list if n["status"] == "rascunho"]
    canceladas = [n for n in notas_list if n["status"] == "cancelada"]
    total_emitido = sum(n["valor"] for n in emitidas)

    return render_template("notas.html",
        notas=notas_list,
        clientes=clientes,
        servicos=servicos,
        n_emitidas=len(emitidas),
        n_rascunhos=len(rascunhos),
        n_canceladas=len(canceladas),
        total_emitido=total_emitido,
        hoje=date.today().isoformat(),
        f_status=f_status,
        f_cliente=f_cliente,
        f_servico=f_servico,
        f_periodo=f_periodo,
    )

def _criar_nota(conn, cliente_id, servico_id, descricao, valor, data_emissao, faturamento_id=None, status="rascunho"):
    cur = conn.execute(
        "INSERT INTO notas_fiscais (faturamento_id, cliente_id, servico_id, numero_nota, descricao, valor, data_emissao, status) VALUES (?,?,?,?,?,?,?,?)",
        (faturamento_id, cliente_id, servico_id, "TEMP", descricao, valor, data_emissao, status)
    )
    nota_id = cur.lastrowid
    numero  = f"NF-{nota_id:06d}"
    conn.execute("UPDATE notas_fiscais SET numero_nota=? WHERE id=?", (numero, nota_id))
    return nota_id

@app.route("/notas/criar", methods=["POST"])
def nota_criar():
    cliente_id    = request.form.get("cliente_id")
    servico_id    = request.form.get("servico_id")
    descricao     = request.form.get("descricao", "").strip() or None
    valor         = float(request.form.get("valor", 0) or 0)
    data_emissao  = request.form.get("data_emissao") or date.today().isoformat()
    status        = request.form.get("status", "rascunho")
    if cliente_id and servico_id and valor > 0:
        conn = get_db()
        nota_id = _criar_nota(conn, cliente_id, servico_id, descricao, valor, data_emissao, status=status)
        conn.commit()
        conn.close()
        return redirect(url_for("nota_ver", id=nota_id))
    return redirect(url_for("notas"))

@app.route("/notas/gerar/<int:fat_id>")
def nota_gerar(fat_id):
    conn = get_db()
    # Verifica se já existe nota para este faturamento
    existe = conn.execute("SELECT id FROM notas_fiscais WHERE faturamento_id=?", (fat_id,)).fetchone()
    if existe:
        conn.close()
        return redirect(url_for("nota_ver", id=existe["id"]))
    fat = conn.execute("""
        SELECT f.*, c.nome as cliente_nome, s.nome as servico_nome
        FROM faturamentos f
        JOIN clientes c ON c.id = f.cliente_id
        JOIN servicos s ON s.id = f.servico_id
        WHERE f.id=?
    """, (fat_id,)).fetchone()
    if not fat:
        conn.close()
        return redirect(url_for("gestao"))
    descricao = fat["descricao"] or f"Referente a: {fat['servico_nome']}"
    nota_id = _criar_nota(conn, fat["cliente_id"], fat["servico_id"],
                          descricao, fat["valor"], fat["data"],
                          faturamento_id=fat_id, status="rascunho")
    conn.commit()
    conn.close()
    return redirect(url_for("nota_ver", id=nota_id))

@app.route("/notas/ver/<int:id>")
def nota_ver(id):
    conn = get_db()
    nota = conn.execute(_nota_join() + " WHERE n.id=?", (id,)).fetchone()
    conn.close()
    if not nota:
        return redirect(url_for("notas"))
    return render_template("ver_nota.html", nota=nota)

@app.route("/notas/editar/<int:id>", methods=["GET", "POST"])
def nota_editar(id):
    conn = get_db()
    nota = conn.execute(_nota_join() + " WHERE n.id=?", (id,)).fetchone()
    if not nota or nota["status"] != "rascunho":
        conn.close()
        return redirect(url_for("notas"))
    if request.method == "POST":
        cliente_id   = request.form.get("cliente_id")
        servico_id   = request.form.get("servico_id")
        descricao    = request.form.get("descricao", "").strip() or None
        valor        = float(request.form.get("valor", 0) or 0)
        data_emissao = request.form.get("data_emissao") or date.today().isoformat()
        conn.execute(
            "UPDATE notas_fiscais SET cliente_id=?, servico_id=?, descricao=?, valor=?, data_emissao=? WHERE id=?",
            (cliente_id, servico_id, descricao, valor, data_emissao, id)
        )
        conn.commit()
        conn.close()
        return redirect(url_for("nota_ver", id=id))
    clientes = conn.execute("SELECT * FROM clientes ORDER BY nome").fetchall()
    servicos = conn.execute("SELECT * FROM servicos ORDER BY nome").fetchall()
    conn.close()
    return render_template("editar_nota.html", nota=nota, clientes=clientes, servicos=servicos)

@app.route("/notas/emitir/<int:id>")
def nota_emitir(id):
    conn = get_db()
    conn.execute("UPDATE notas_fiscais SET status='emitida' WHERE id=? AND status='rascunho'", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("nota_ver", id=id))

@app.route("/notas/cancelar/<int:id>")
def nota_cancelar(id):
    conn = get_db()
    conn.execute("UPDATE notas_fiscais SET status='cancelada' WHERE id=? AND status != 'cancelada'", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("nota_ver", id=id))

@app.route("/notas/excluir/<int:id>")
def nota_excluir(id):
    conn = get_db()
    conn.execute("DELETE FROM notas_fiscais WHERE id=? AND status='rascunho'", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("notas"))

# ── Pesquisa Global ────────────────────────────────────────────────────────────

@app.route("/busca")
def busca():
    q = request.args.get("q", "").strip()
    resultados = {}
    total = 0

    if q:
        conn = get_db()
        like = f"%{q}%"

        resultados["lancamentos"] = conn.execute(
            "SELECT * FROM lancamentos WHERE descricao LIKE ? OR categoria LIKE ? ORDER BY data DESC LIMIT 30",
            (like, like)
        ).fetchall()

        resultados["clientes"] = conn.execute(
            "SELECT * FROM clientes WHERE nome LIKE ? OR cpf_cnpj LIKE ? OR email LIKE ? ORDER BY nome LIMIT 20",
            (like, like, like)
        ).fetchall()

        resultados["servicos"] = conn.execute(
            "SELECT * FROM servicos WHERE nome LIKE ? OR descricao LIKE ? ORDER BY nome LIMIT 20",
            (like, like)
        ).fetchall()

        resultados["faturamentos"] = conn.execute(
            """SELECT f.*, c.nome as cliente_nome, s.nome as servico_nome
               FROM faturamentos f
               JOIN clientes c ON c.id = f.cliente_id
               JOIN servicos s ON s.id = f.servico_id
               WHERE c.nome LIKE ? OR s.nome LIKE ? OR f.descricao LIKE ?
               ORDER BY f.data DESC LIMIT 20""",
            (like, like, like)
        ).fetchall()

        resultados["notas"] = conn.execute(
            _nota_join() + " WHERE n.numero_nota LIKE ? OR c.nome LIKE ? OR s.nome LIKE ? OR n.descricao LIKE ? ORDER BY n.id DESC LIMIT 20",
            (like, like, like, like)
        ).fetchall()

        resultados["metas"] = conn.execute(
            "SELECT * FROM metas WHERE nome LIKE ? OR descricao LIKE ? ORDER BY nome LIMIT 20",
            (like, like)
        ).fetchall()

        conn.close()
        total = sum(len(v) for v in resultados.values())

    return render_template("busca.html", q=q, resultados=resultados, total=total)

# ── Relatório Mensal ───────────────────────────────────────────────────────────

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

@app.route("/relatorio")
def relatorio():
    hoje_dt = date.today()
    try:
        mes = int(request.args.get("mes", hoje_dt.month))
        ano = int(request.args.get("ano", hoje_dt.year))
        if not (1 <= mes <= 12) or ano < 2000:
            raise ValueError
    except (ValueError, TypeError):
        mes, ano = hoje_dt.month, hoje_dt.year

    _, ultimo_dia = calendar.monthrange(ano, mes)
    data_ini = f"{ano}-{mes:02d}-01"
    data_fim = f"{ano}-{mes:02d}-{ultimo_dia:02d}"

    conn = get_db()

    lancs = conn.execute(
        "SELECT * FROM lancamentos WHERE data BETWEEN ? AND ?", (data_ini, data_fim)
    ).fetchall()

    total_entradas  = sum(l["valor"] for l in lancs if l["tipo"] == "entrada")
    total_saidas    = sum(l["valor"] for l in lancs if l["tipo"] == "despesa")
    saldo_mes       = total_entradas - total_saidas
    total_fixas     = sum(l["valor"] for l in lancs if l["tipo"] == "despesa" and l["subtipo"] == "fixa")
    total_variaveis = sum(l["valor"] for l in lancs if l["tipo"] == "despesa" and l["subtipo"] == "variável")
    qtd_lancamentos = len(lancs)

    fats = conn.execute(
        "SELECT * FROM faturamentos WHERE data BETWEEN ? AND ?", (data_ini, data_fim)
    ).fetchall()
    total_faturado   = sum(f["valor"] for f in fats)
    total_recebido   = sum(f["valor"] for f in fats if f["status"] == "pago")
    total_aberto     = sum(f["valor"] for f in fats if f["status"] == "pendente")
    qtd_faturamentos = len(fats)

    nts = conn.execute(
        "SELECT * FROM notas_fiscais WHERE data_emissao BETWEEN ? AND ?", (data_ini, data_fim)
    ).fetchall()
    notas_emitidas   = len([n for n in nts if n["status"] == "emitida"])
    notas_canceladas = len([n for n in nts if n["status"] == "cancelada"])
    notas_rascunhos  = len([n for n in nts if n["status"] == "rascunho"])
    valor_notas      = sum(n["valor"] for n in nts if n["status"] == "emitida")

    metas_todas, reserva, patrimonial, _ = calcular_patrimonio(conn)
    conn.close()

    metas_concluidas = [m for m in metas_todas if m["valor_atual"] >= m["valor_objetivo"] and m["valor_objetivo"] > 0]
    metas_andamento  = [m for m in metas_todas if m["valor_atual"] < m["valor_objetivo"]]
    valor_guardado   = sum(m["valor_atual"] for m in metas_todas)

    frases_mes = []
    if saldo_mes > 0:
        frases_mes.append(f"✅ Este foi um mês positivo, com saldo de R$ {saldo_mes:.2f}.")
    elif saldo_mes < 0:
        frases_mes.append(f"⚠ Este mês encerrou no negativo (R$ {abs(saldo_mes):.2f}). Atenção aos gastos.")
    else:
        frases_mes.append("📊 O mês fechou no zero a zero — entradas e saídas se igualaram.")

    if total_fixas > total_variaveis and total_fixas > 0:
        frases_mes.append("📌 As despesas fixas foram as maiores do mês.")
    if total_variaveis > 0 and total_variaveis >= total_fixas:
        frases_mes.append("⚠ Atenção: despesas variáveis pesaram no resultado.")

    if total_faturado > 0:
        pct = (total_recebido / total_faturado * 100)
        if pct >= 80:
            frases_mes.append(f"💰 Ótimo recebimento: {pct:.0f}% dos faturamentos foram pagos.")
        elif pct >= 50:
            frases_mes.append(f"💰 {pct:.0f}% dos faturamentos foram recebidos. Acompanhe os pendentes.")
        elif pct > 0:
            frases_mes.append(f"⚠ Apenas {pct:.0f}% dos faturamentos foram recebidos.")
        else:
            frases_mes.append("⚠ Nenhum faturamento foi recebido neste mês.")

    if metas_concluidas:
        frases_mes.append(f"🎯 Parabéns! {len(metas_concluidas)} meta(s) financeira(s) concluída(s).")
    elif metas_andamento:
        frases_mes.append(f"🎯 {len(metas_andamento)} meta(s) em andamento. Continue avançando!")

    if total_entradas > 0 and saldo_mes > 0:
        pct_g = saldo_mes / total_entradas * 100
        if pct_g >= 20:
            frases_mes.append(f"⭐ Excelente! Você poupou {pct_g:.0f}% das entradas deste mês.")
        elif pct_g >= 10:
            frases_mes.append(f"👍 Você poupou {pct_g:.0f}% das entradas. Meta: chegar a 20%.")

    anos_disponiveis = list(range(max(2020, hoje_dt.year - 5), hoje_dt.year + 2))

    return render_template("relatorio.html",
        mes=mes, ano=ano,
        nome_mes=MESES_PT.get(mes, str(mes)),
        data_ini=data_ini, data_fim=data_fim,
        total_entradas=total_entradas,
        total_saidas=total_saidas,
        saldo_mes=saldo_mes,
        total_fixas=total_fixas,
        total_variaveis=total_variaveis,
        qtd_lancamentos=qtd_lancamentos,
        total_faturado=total_faturado,
        total_recebido=total_recebido,
        total_aberto=total_aberto,
        qtd_faturamentos=qtd_faturamentos,
        notas_emitidas=notas_emitidas,
        notas_canceladas=notas_canceladas,
        notas_rascunhos=notas_rascunhos,
        valor_notas=valor_notas,
        metas=metas_todas,
        metas_concluidas=len(metas_concluidas),
        metas_andamento=len(metas_andamento),
        valor_guardado=valor_guardado,
        reserva=reserva,
        patrimonial=patrimonial,
        frases_mes=frases_mes,
        anos_disponiveis=anos_disponiveis,
        hoje=hoje_dt.isoformat(),
        MESES_PT=MESES_PT,
    )

# ── Exportar nota fiscal como HTML baixável ────────────────────────────────────

@app.route("/notas/exportar/<int:id>")
def nota_exportar(id):
    conn = get_db()
    nota = conn.execute(_nota_join() + " WHERE n.id=?", (id,)).fetchone()
    conn.close()
    if not nota:
        return redirect(url_for("notas"))
    html = render_template("nota_export.html", nota=nota)
    buffer = io.BytesIO(html.encode("utf-8"))
    buffer.seek(0)
    nome_arquivo = f"nota_{nota['numero_nota']}.html"
    return send_file(buffer, as_attachment=True, download_name=nome_arquivo, mimetype="text/html")

# ── Constantes de backup ───────────────────────────────────────────────────────

PASTAS_IGNORADAS = {
    "node_modules", "venv", "env", "__pycache__", ".git",
    ".cache", "dist", "build", "tmp", "temp", "logs",
    "uploads", "backups", ".local", ".upm", "attached_assets",
}

EXTENSOES_PERMITIDAS = {
    ".py", ".html", ".css", ".js", ".txt", ".md",
    ".json", ".yaml", ".yml", ".toml", ".cfg", ".ini", ".env",
}

ARQUIVOS_RAIZ_PERMITIDOS = {
    "requirements.txt", "README.md", "README.txt",
    "pyproject.toml", "setup.py", "setup.cfg",
    ".replit", "replit.nix", "replit.md",
}

BACKUP_IMPORT_RAIZ = {"app.py", "database.db", "main.py", "pyproject.toml", "replit.md"}
BACKUP_IMPORT_PASTAS = {"templates", "static"}

# ── Backup do banco de dados (legado — mantido) ────────────────────────────────

@app.route("/backup")
def backup():
    return send_file(DB, as_attachment=True, download_name="backup_financeiro.db")

# ── Download do projeto (código-fonte em zip — legado) ────────────────────────

@app.route("/download-project")
def download_project():
    raiz = os.path.abspath(os.path.dirname(__file__))
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for dirpath, dirnames, filenames in os.walk(raiz):
            dirnames[:] = [d for d in dirnames if d not in PASTAS_IGNORADAS and not d.startswith(".")]
            rel_dir = os.path.relpath(dirpath, raiz)
            for filename in filenames:
                if filename.lower().endswith(".zip") or filename == "database.db":
                    continue
                ext = os.path.splitext(filename)[1].lower()
                if rel_dir == ".":
                    if filename not in ARQUIVOS_RAIZ_PERMITIDOS and ext not in EXTENSOES_PERMITIDAS:
                        continue
                else:
                    if ext not in EXTENSOES_PERMITIDAS:
                        continue
                arcname = filename if rel_dir == "." else os.path.join(rel_dir, filename)
                zf.write(os.path.join(dirpath, filename), arcname)
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name="site_backup_atual.zip", mimetype="application/zip")

# ── Sistema de Backup — página de gestão ──────────────────────────────────────

@app.route("/sistema/backup")
def backup_pagina():
    db_exists = os.path.exists(DB)
    db_size_kb = round(os.path.getsize(DB) / 1024, 1) if db_exists else 0
    return render_template("backup_sistema.html", db_size_kb=db_size_kb)

# ── Backup completo (código + banco) com data/hora no nome ────────────────────

@app.route("/backup/completo")
def backup_completo():
    raiz = os.path.abspath(os.path.dirname(__file__))
    agora = datetime.now().strftime("%Y-%m-%d_%H-%M")
    buffer = io.BytesIO()

    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        # Incluir banco de dados
        db_path = os.path.join(raiz, DB)
        if os.path.exists(db_path):
            zf.write(db_path, "database.db")

        # Incluir arquivos do projeto
        for dirpath, dirnames, filenames in os.walk(raiz):
            dirnames[:] = [d for d in dirnames if d not in PASTAS_IGNORADAS and not d.startswith(".")]
            rel_dir = os.path.relpath(dirpath, raiz)
            for filename in filenames:
                if filename.lower().endswith(".zip") or filename == "database.db":
                    continue
                ext = os.path.splitext(filename)[1].lower()
                if rel_dir == ".":
                    if filename not in ARQUIVOS_RAIZ_PERMITIDOS and ext not in EXTENSOES_PERMITIDAS:
                        continue
                else:
                    if ext not in EXTENSOES_PERMITIDAS:
                        continue
                arcname = filename if rel_dir == "." else os.path.join(rel_dir, filename)
                zf.write(os.path.join(dirpath, filename), arcname)

    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"backup_{agora}.zip",
        mimetype="application/zip",
    )

# ── Importar/restaurar backup ──────────────────────────────────────────────────

@app.route("/backup/importar", methods=["POST"])
def backup_importar():
    arquivo = request.files.get("arquivo")

    if not arquivo or not arquivo.filename:
        return render_template("backup_sistema.html", db_size_kb=0,
            msg_erro="Nenhum arquivo enviado.")

    if not arquivo.filename.lower().endswith(".zip"):
        return render_template("backup_sistema.html", db_size_kb=0,
            msg_erro="O arquivo deve ser um .zip válido.")

    raiz = os.path.abspath(os.path.dirname(__file__))
    tmpdir = tempfile.mkdtemp()

    try:
        tmp_zip = os.path.join(tmpdir, "import.zip")
        arquivo.save(tmp_zip)

        sucesso = []
        erros = []

        with zipfile.ZipFile(tmp_zip, "r") as zf:
            for nome in zf.namelist():
                nome_norm = nome.replace("\\", "/").strip("/")

                # Segurança: bloquear path traversal
                if ".." in nome_norm or nome_norm.startswith("/"):
                    erros.append(f"Bloqueado (caminho suspeito): {nome}")
                    continue

                partes = nome_norm.split("/")
                raiz_item = partes[0]

                # Arquivo na raiz do zip
                if len(partes) == 1:
                    if raiz_item in BACKUP_IMPORT_RAIZ and not nome_norm.endswith("/"):
                        zf.extract(nome, tmpdir)
                        src = os.path.join(tmpdir, raiz_item)
                        dst = os.path.join(raiz, raiz_item)
                        if os.path.isfile(src):
                            shutil.copy2(src, dst)
                            sucesso.append(raiz_item)
                    else:
                        erros.append(f"Ignorado: {nome}")

                # Arquivo dentro de pasta permitida
                elif raiz_item in BACKUP_IMPORT_PASTAS:
                    ext = os.path.splitext(nome_norm)[1].lower()
                    if ext not in EXTENSOES_PERMITIDAS:
                        erros.append(f"Ignorado (extensão não permitida): {nome}")
                        continue
                    zf.extract(nome, tmpdir)
                    src = os.path.join(tmpdir, *partes)
                    dst = os.path.join(raiz, *partes)
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    if os.path.isfile(src):
                        shutil.copy2(src, dst)
                        sucesso.append(nome_norm)
                else:
                    erros.append(f"Ignorado: {nome}")

        msg = f"Importação concluída: {len(sucesso)} arquivo(s) restaurado(s)."
        if erros:
            msg += f" {len(erros)} item(ns) ignorado(s) por segurança."

        db_exists = os.path.exists(DB)
        db_size_kb = round(os.path.getsize(DB) / 1024, 1) if db_exists else 0
        return render_template("backup_sistema.html", db_size_kb=db_size_kb,
            msg_sucesso=msg, detalhes_sucesso=sucesso, detalhes_erro=erros)

    except zipfile.BadZipFile:
        return render_template("backup_sistema.html", db_size_kb=0,
            msg_erro="Arquivo .zip inválido ou corrompido.")
    except Exception as e:
        return render_template("backup_sistema.html", db_size_kb=0,
            msg_erro=f"Erro durante a importação: {str(e)}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

# ── Integração Fiscal ─────────────────────────────────────────────────────────

@app.route("/integracao-fiscal", methods=["GET", "POST"])
def integracao_fiscal():
    conn = get_db()
    if request.method == "POST":
        modo_emissao     = request.form.get("modo_emissao", "interno")
        provedor         = request.form.get("provedor", "").strip() or None
        ambiente         = request.form.get("ambiente", "homologacao")
        municipio_codigo = request.form.get("municipio_codigo", "").strip() or None
        observacoes      = request.form.get("observacoes", "").strip() or None

        existente = conn.execute("SELECT id FROM integracao_fiscal_config ORDER BY id DESC LIMIT 1").fetchone()
        if existente:
            conn.execute("""
                UPDATE integracao_fiscal_config SET
                    modo_emissao=?, provedor=?, ambiente=?, municipio_codigo=?, observacoes=?
                WHERE id=?
            """, (modo_emissao, provedor, ambiente, municipio_codigo, observacoes, existente["id"]))
        else:
            conn.execute("""
                INSERT INTO integracao_fiscal_config
                    (modo_emissao, provedor, ambiente, municipio_codigo, observacoes)
                VALUES (?,?,?,?,?)
            """, (modo_emissao, provedor, ambiente, municipio_codigo, observacoes))
        conn.commit()
        conn.close()
        return redirect(url_for("integracao_fiscal") + "?salvo=1")

    config_integ  = conn.execute("SELECT * FROM integracao_fiscal_config ORDER BY id DESC LIMIT 1").fetchone()
    config_fiscal = conn.execute("SELECT * FROM configuracao_fiscal ORDER BY id DESC LIMIT 1").fetchone()
    campos_faltando = validar_config_fiscal(config_fiscal)

    # Contadores de status de integração
    notas_por_status = {}
    rows = conn.execute(
        "SELECT status_integracao, COUNT(*) as qtd FROM notas_fiscais GROUP BY status_integracao"
    ).fetchall()
    for r in rows:
        notas_por_status[r["status_integracao"]] = r["qtd"]

    conn.close()
    salvo = request.args.get("salvo") == "1"
    return render_template("integracao_fiscal.html",
        config_integ=config_integ,
        config_fiscal=config_fiscal,
        campos_faltando=campos_faltando,
        notas_por_status=notas_por_status,
        salvo=salvo,
        LABEL_INTEGRACAO=LABEL_INTEGRACAO,
    )


@app.route("/notas/preparar/<int:id>", methods=["POST"])
def nota_preparar(id):
    conn = get_db()
    nota = conn.execute(_nota_join() + " WHERE n.id=?", (id,)).fetchone()
    if not nota:
        conn.close()
        return redirect(url_for("notas"))

    config_fiscal = conn.execute("SELECT * FROM configuracao_fiscal ORDER BY id DESC LIMIT 1").fetchone()
    config_integ  = conn.execute("SELECT * FROM integracao_fiscal_config ORDER BY id DESC LIMIT 1").fetchone()
    campos_faltando = validar_config_fiscal(config_fiscal)

    if campos_faltando:
        mensagem = "Configuração fiscal incompleta: " + ", ".join(campos_faltando)
        registrar_evento_fiscal(conn, id, "preparacao", "erro_validacao",
                                mensagem=mensagem)
        conn.execute(
            "UPDATE notas_fiscais SET status_integracao='nao_enviada' WHERE id=?", (id,)
        )
        conn.commit()
        conn.close()
        return redirect(url_for("nota_ver", id=id) + "?msg_integ=erro_config")

    payload = montar_payload_fiscal(nota, config_fiscal, config_integ)
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)

    conn.execute(
        "UPDATE notas_fiscais SET status_integracao='pronta_para_envio', payload_fiscal=? WHERE id=?",
        (payload_json, id)
    )
    registrar_evento_fiscal(conn, id, "preparacao", "pronta_para_envio",
                            mensagem="Dados validados. Nota pronta para integração.",
                            payload=payload)
    conn.commit()
    conn.close()
    return redirect(url_for("nota_ver", id=id) + "?msg_integ=pronta")


@app.route("/notas/simular/<int:id>", methods=["POST"])
def nota_simular(id):
    conn = get_db()
    nota = conn.execute(_nota_join() + " WHERE n.id=?", (id,)).fetchone()
    if not nota:
        conn.close()
        return redirect(url_for("notas"))

    config_fiscal = conn.execute("SELECT * FROM configuracao_fiscal ORDER BY id DESC LIMIT 1").fetchone()
    config_integ  = conn.execute("SELECT * FROM integracao_fiscal_config ORDER BY id DESC LIMIT 1").fetchone()
    campos_faltando = validar_config_fiscal(config_fiscal)

    if campos_faltando:
        mensagem = "Simulação falhou — configuração fiscal incompleta: " + ", ".join(campos_faltando)
        registrar_evento_fiscal(conn, id, "simulacao", "rejeitada", mensagem=mensagem)
        conn.execute("UPDATE notas_fiscais SET status_integracao='rejeitada' WHERE id=?", (id,))
        conn.commit()
        conn.close()
        return redirect(url_for("nota_ver", id=id) + "?msg_integ=simul_erro")

    payload = montar_payload_fiscal(nota, config_fiscal, config_integ)
    protocolo_simulado = "SIM-" + str(uuid.uuid4()).upper()[:12]

    ambiente = config_integ["ambiente"] if config_integ else "homologacao"
    status_novo = "enviada_homologacao" if ambiente == "homologacao" else "enviada_producao"

    resposta_simulada = {
        "protocolo":  protocolo_simulado,
        "status":     "autorizada_simulacao",
        "ambiente":   ambiente,
        "mensagem":   "Envio simulado com sucesso. Nenhuma API real foi acionada.",
        "timestamp":  datetime.now().isoformat(),
    }

    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    conn.execute(
        "UPDATE notas_fiscais SET status_integracao=?, payload_fiscal=? WHERE id=?",
        (status_novo, payload_json, id)
    )
    registrar_evento_fiscal(conn, id, "simulacao", status_novo,
                            mensagem="Envio simulado com sucesso. Protocolo fictício gerado.",
                            protocolo=protocolo_simulado,
                            payload=payload,
                            resposta=resposta_simulada)
    conn.commit()
    conn.close()
    return redirect(url_for("nota_ver", id=id) + "?msg_integ=simul_ok")


@app.route("/status-fiscal")
def status_fiscal():
    conn = get_db()
    config_fiscal   = conn.execute("SELECT * FROM configuracao_fiscal ORDER BY id DESC LIMIT 1").fetchone()
    config_integ    = conn.execute("SELECT * FROM integracao_fiscal_config ORDER BY id DESC LIMIT 1").fetchone()
    campos_faltando = validar_config_fiscal(config_fiscal)

    notas_list = conn.execute("""
        SELECT n.*, c.nome as cliente_nome, s.nome as servico_nome
        FROM notas_fiscais n
        JOIN clientes c ON c.id = n.cliente_id
        JOIN servicos s ON s.id = n.servico_id
        ORDER BY n.id DESC
    """).fetchall()

    # Último evento por nota
    ultimos_eventos = {}
    evs = conn.execute(
        "SELECT * FROM eventos_fiscais ORDER BY created_at DESC"
    ).fetchall()
    for ev in evs:
        if ev["nota_id"] not in ultimos_eventos:
            ultimos_eventos[ev["nota_id"]] = ev

    conn.close()

    contadores = {}
    for n in notas_list:
        si = n["status_integracao"] or "nao_enviada"
        contadores[si] = contadores.get(si, 0) + 1

    return render_template("status_fiscal.html",
        notas=notas_list,
        config_fiscal=config_fiscal,
        config_integ=config_integ,
        campos_faltando=campos_faltando,
        ultimos_eventos=ultimos_eventos,
        contadores=contadores,
        LABEL_INTEGRACAO=LABEL_INTEGRACAO,
    )


@app.route("/notas/eventos/<int:id>")
def nota_eventos(id):
    conn = get_db()
    nota     = conn.execute(_nota_join() + " WHERE n.id=?", (id,)).fetchone()
    eventos  = conn.execute(
        "SELECT * FROM eventos_fiscais WHERE nota_id=? ORDER BY created_at DESC", (id,)
    ).fetchall()
    conn.close()
    if not nota:
        return redirect(url_for("notas"))
    return render_template("nota_eventos.html",
        nota=nota,
        eventos=eventos,
        LABEL_INTEGRACAO=LABEL_INTEGRACAO,
    )


# ── Rota de Inteligência Financeira ───────────────────────────────────────────

@app.route("/inteligencia")
def inteligencia():
    conn  = get_db()
    dados = calcular_inteligencia_financeira(conn)
    conn.close()
    faixa, faixa_cls = faixa_score(dados.get("score", 0)) if not dados.get("sem_dados") else ("—", "score-atencao")
    return render_template("inteligencia.html",
        dados=dados,
        faixa_score=faixa,
        faixa_cls=faixa_cls,
    )

# ── Etapa 16: Registrar previsão como lançamento real ─────────────────────────

@app.route("/previsao/registrar", methods=["POST"])
def previsao_registrar():
    """Registra uma previsão de lançamento como lançamento real."""
    descricao = request.form.get("descricao", "").strip()
    categoria = request.form.get("categoria", "outros").strip() or "outros"
    subtipo   = request.form.get("subtipo", "").strip()
    tipo      = request.form.get("tipo", "despesa").strip()
    data      = request.form.get("data", date.today().isoformat()).strip()
    try:
        valor = float(request.form.get("valor", "0").replace(",", "."))
    except Exception:
        valor = 0.0
    if not descricao or valor <= 0:
        flash("Preencha todos os campos obrigatórios.", "error")
        return redirect("/inteligencia")
    conn = get_db()
    existe = conn.execute(
        "SELECT id FROM lancamentos WHERE data=? AND descricao=? AND ABS(valor-?)<0.01 AND tipo=?",
        (data, descricao, valor, tipo)
    ).fetchone()
    if existe:
        flash(f"Lançamento '{descricao}' já existe para essa data e valor.", "warning")
        conn.close()
        return redirect("/inteligencia")
    conn.execute(
        "INSERT INTO lancamentos (tipo, data, descricao, categoria, valor, subtipo) VALUES (?,?,?,?,?,?)",
        (tipo, data, descricao, categoria, valor, subtipo)
    )
    # Atualizar padrão aprendido: confirma que o padrão é válido
    chave = descricao[:20].strip().lower()
    conn.execute("""
        UPDATE padroes_aprendidos SET frequencia = frequencia + 1, ultima_ocorrencia=?
        WHERE chave_referencia=?
    """, (date.today().isoformat(), chave))
    conn.commit()
    conn.close()
    flash(f"Lançamento '{descricao}' registrado com sucesso (R$ {valor:.2f}).", "success")
    return redirect("/inteligencia")


# ── Configuração Fiscal ────────────────────────────────────────────────────────

@app.route("/configuracao-fiscal", methods=["GET", "POST"])
def configuracao_fiscal():
    conn = get_db()
    if request.method == "POST":
        razao_social       = request.form.get("razao_social", "").strip() or None
        cpf_cnpj           = request.form.get("cpf_cnpj", "").strip() or None
        inscricao_municipal= request.form.get("inscricao_municipal", "").strip() or None
        inscricao_estadual = request.form.get("inscricao_estadual", "").strip() or None
        email              = request.form.get("email", "").strip() or None
        telefone           = request.form.get("telefone", "").strip() or None
        endereco           = request.form.get("endereco", "").strip() or None
        cidade             = request.form.get("cidade", "").strip() or None
        estado             = request.form.get("estado", "").strip() or None
        cep                = request.form.get("cep", "").strip() or None
        regime_tributario  = request.form.get("regime_tributario", "").strip() or None
        observacoes        = request.form.get("observacoes", "").strip() or None

        existente = conn.execute("SELECT id FROM configuracao_fiscal ORDER BY id DESC LIMIT 1").fetchone()
        if existente:
            conn.execute("""
                UPDATE configuracao_fiscal SET
                    razao_social=?, cpf_cnpj=?, inscricao_municipal=?, inscricao_estadual=?,
                    email=?, telefone=?, endereco=?, cidade=?, estado=?, cep=?,
                    regime_tributario=?, observacoes=?
                WHERE id=?
            """, (razao_social, cpf_cnpj, inscricao_municipal, inscricao_estadual,
                  email, telefone, endereco, cidade, estado, cep,
                  regime_tributario, observacoes, existente["id"]))
        else:
            conn.execute("""
                INSERT INTO configuracao_fiscal
                    (razao_social, cpf_cnpj, inscricao_municipal, inscricao_estadual,
                     email, telefone, endereco, cidade, estado, cep, regime_tributario, observacoes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (razao_social, cpf_cnpj, inscricao_municipal, inscricao_estadual,
                  email, telefone, endereco, cidade, estado, cep,
                  regime_tributario, observacoes))
        conn.commit()
        conn.close()
        return redirect(url_for("configuracao_fiscal") + "?salvo=1")

    config = conn.execute("SELECT * FROM configuracao_fiscal ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    salvo = request.args.get("salvo") == "1"
    return render_template("configuracao_fiscal.html", config=config, salvo=salvo)


# ── APIs de automação ────────────────────────────────────────────────────────

from flask import jsonify

@app.route("/api/sugestao-categoria")
def api_sugestao_categoria():
    descricao = request.args.get("descricao", "").strip()
    if len(descricao) < 3:
        return jsonify({})
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as n FROM lancamentos").fetchone()["n"]
    sug = sugerir_categoria(descricao, conn)
    conn.close()
    if not sug or not sug.get("categoria"):
        return jsonify({})
    resultado = {
        "categoria": sug["categoria"],
        "confianca": sug.get("confianca", "baixa"),
    }
    if sug.get("subtipo"):
        resultado["subtipo"] = sug["subtipo"]
    if sug.get("valor"):
        resultado["valor"] = sug["valor"]
    if total < 10:
        resultado["aviso"] = "As sugestões melhorarão conforme você registrar mais lançamentos."
    return jsonify(resultado)


@app.route("/api/autocomplete-descricao")
def api_autocomplete_descricao():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])
    conn = get_db()
    rows = conn.execute(
        """SELECT descricao, categoria, subtipo, tipo,
                  AVG(valor) as valor_medio, COUNT(*) as freq
           FROM lancamentos
           WHERE descricao LIKE ?
           GROUP BY descricao, categoria, subtipo, tipo
           ORDER BY freq DESC, MAX(id) DESC
           LIMIT 8""",
        (f"%{q}%",)
    ).fetchall()
    conn.close()
    return jsonify([{
        "descricao": r["descricao"],
        "categoria": r["categoria"],
        "subtipo":   r["subtipo"] or "",
        "tipo":      r["tipo"],
        "valor":     round(r["valor_medio"], 2),
    } for r in rows])


@app.route("/api/servico-valor/<int:id>")
def api_servico_valor(id):
    conn = get_db()
    s = conn.execute("SELECT valor_padrao FROM servicos WHERE id=?", (id,)).fetchone()
    conn.close()
    if s and s["valor_padrao"]:
        return jsonify({"valor": s["valor_padrao"]})
    return jsonify({})


# ── Lançamentos Recorrentes ───────────────────────────────────────────────────

@app.route("/recorrentes")
def recorrentes():
    conn = get_db()
    todos = conn.execute(
        "SELECT * FROM lancamentos_recorrentes ORDER BY ativo DESC, proxima_data ASC"
    ).fetchall()
    conn.close()
    hoje = date.today().isoformat()
    msg = request.args.get("msg", "")
    return render_template("recorrentes.html", recorrentes=todos, hoje=hoje, msg=msg)


@app.route("/recorrentes/adicionar", methods=["POST"])
def recorrente_adicionar():
    tipo         = request.form.get("tipo", "despesa")
    descricao    = request.form.get("descricao", "").strip()
    categoria    = request.form.get("categoria", "").strip()
    valor        = float(request.form.get("valor", 0) or 0)
    subtipo      = request.form.get("subtipo", "").strip() or None
    frequencia   = request.form.get("frequencia", "mensal")
    proxima_data = request.form.get("proxima_data") or date.today().isoformat()
    if descricao and categoria and valor > 0:
        conn = get_db()
        conn.execute(
            "INSERT INTO lancamentos_recorrentes (tipo,descricao,categoria,valor,subtipo,frequencia,proxima_data,ativo) VALUES (?,?,?,?,?,?,?,1)",
            (tipo, descricao, categoria, valor, subtipo, frequencia, proxima_data)
        )
        conn.commit()
        conn.close()
    return redirect(url_for("recorrentes") + "?msg=adicionado")


@app.route("/recorrentes/editar/<int:id>", methods=["POST"])
def recorrente_editar(id):
    tipo         = request.form.get("tipo", "despesa")
    descricao    = request.form.get("descricao", "").strip()
    categoria    = request.form.get("categoria", "").strip()
    valor        = float(request.form.get("valor", 0) or 0)
    subtipo      = request.form.get("subtipo", "").strip() or None
    frequencia   = request.form.get("frequencia", "mensal")
    proxima_data = request.form.get("proxima_data") or date.today().isoformat()
    if descricao and categoria and valor > 0:
        conn = get_db()
        conn.execute(
            "UPDATE lancamentos_recorrentes SET tipo=?,descricao=?,categoria=?,valor=?,subtipo=?,frequencia=?,proxima_data=? WHERE id=?",
            (tipo, descricao, categoria, valor, subtipo, frequencia, proxima_data, id)
        )
        conn.commit()
        conn.close()
    return redirect(url_for("recorrentes") + "?msg=editado")


@app.route("/recorrentes/toggle/<int:id>")
def recorrente_toggle(id):
    conn = get_db()
    rec = conn.execute("SELECT ativo FROM lancamentos_recorrentes WHERE id=?", (id,)).fetchone()
    if rec:
        novo = 0 if rec["ativo"] else 1
        conn.execute("UPDATE lancamentos_recorrentes SET ativo=? WHERE id=?", (novo, id))
        conn.commit()
    conn.close()
    return redirect(url_for("recorrentes"))


@app.route("/recorrentes/excluir/<int:id>")
def recorrente_excluir(id):
    conn = get_db()
    conn.execute("DELETE FROM lancamentos_recorrentes WHERE id=?", (id,))
    conn.commit()
    conn.close()
    return redirect(url_for("recorrentes") + "?msg=excluido")


@app.route("/recorrentes/gerar")
def recorrentes_gerar():
    conn = get_db()
    hoje_str = date.today().isoformat()
    ativos = conn.execute(
        "SELECT * FROM lancamentos_recorrentes WHERE ativo=1 AND proxima_data <= ?",
        (hoje_str,)
    ).fetchall()
    gerados = 0
    for r in ativos:
        existe = conn.execute(
            "SELECT id FROM lancamentos WHERE descricao=? AND data=? AND valor=?",
            (r["descricao"], r["proxima_data"], r["valor"])
        ).fetchone()
        if not existe:
            conn.execute(
                "INSERT INTO lancamentos (tipo, data, descricao, categoria, valor, subtipo) VALUES (?,?,?,?,?,?)",
                (r["tipo"], r["proxima_data"], r["descricao"], r["categoria"], r["valor"], r["subtipo"])
            )
            # Avança a próxima data
            try:
                prox = date.fromisoformat(r["proxima_data"])
                if r["frequencia"] == "mensal":
                    mes  = prox.month + 1 if prox.month < 12 else 1
                    ano  = prox.year if prox.month < 12 else prox.year + 1
                    import calendar as _cal
                    ultimo_dia = _cal.monthrange(ano, mes)[1]
                    nova_data  = prox.replace(year=ano, month=mes, day=min(prox.day, ultimo_dia))
                elif r["frequencia"] == "semanal":
                    nova_data = prox + timedelta(weeks=1)
                else:
                    nova_data = prox + timedelta(days=30)
                conn.execute(
                    "UPDATE lancamentos_recorrentes SET proxima_data=? WHERE id=?",
                    (nova_data.isoformat(), r["id"])
                )
            except Exception:
                pass
            gerados += 1
    conn.commit()
    conn.close()
    return redirect(url_for("recorrentes") + f"?msg=gerados_{gerados}")


# ── Etapa 15: Importação de CSV ───────────────────────────────────────────────

@app.route("/importar", methods=["GET", "POST"])
def importar():
    if request.method == "POST":
        f = request.files.get("csv_file")
        if not f or not f.filename:
            flash("Selecione um arquivo CSV para importar.", "error")
            return redirect("/importar")
        if not f.filename.lower().endswith(".csv"):
            flash("O arquivo deve ter extensão .csv", "error")
            return redirect("/importar")
        try:
            content = f.read().decode("utf-8-sig", errors="ignore")
        except Exception:
            flash("Não foi possível ler o arquivo. Verifique a codificação (UTF-8).", "error")
            return redirect("/importar")
        linhas, erros = _parsear_csv(content)
        if not linhas and not erros:
            flash("O arquivo CSV está vazio ou sem colunas reconhecíveis.", "warning")
            return redirect("/importar")
        # Enriquecer com sugestão de categoria
        conn = get_db()
        for linha in linhas:
            if not linha["categoria"] and linha["descricao"]:
                sug = sugerir_categoria(linha["descricao"], conn)
                if sug and sug.get("categoria"):
                    linha["categoria"]     = sug["categoria"]
                    linha["subtipo"]       = sug.get("subtipo", linha["subtipo"])
                    linha["cat_sugerida"]  = True
                else:
                    linha["cat_sugerida"] = False
            else:
                linha["cat_sugerida"] = False
        conn.close()
        return render_template("importar.html", step="preview",
                               linhas=linhas, erros=erros,
                               total=len(linhas), total_erros=len(erros))
    return render_template("importar.html", step="upload")


@app.route("/importar/confirmar", methods=["POST"])
def importar_confirmar():
    conn  = get_db()
    n_imp = 0
    n_dup = 0
    n_err = 0
    indices = request.form.getlist("idx")
    for idx in indices:
        data    = request.form.get(f"data_{idx}",      "").strip()
        desc    = request.form.get(f"descricao_{idx}", "").strip()
        valor_s = request.form.get(f"valor_{idx}",     "0").strip()
        tipo    = request.form.get(f"tipo_{idx}",      "despesa").strip()
        cat     = request.form.get(f"categoria_{idx}", "outros").strip() or "outros"
        sub     = request.form.get(f"subtipo_{idx}",   "").strip()
        try:
            valor = float(valor_s)
        except Exception:
            n_err += 1
            continue
        if not data or valor <= 0:
            n_err += 1
            continue
        # Verificar duplicata
        existe = conn.execute(
            "SELECT id FROM lancamentos WHERE data=? AND descricao=? AND ABS(valor-?)< 0.01 AND tipo=?",
            (data, desc, valor, tipo)
        ).fetchone()
        if existe:
            n_dup += 1
            continue
        conn.execute(
            "INSERT INTO lancamentos (tipo, data, descricao, categoria, valor, subtipo) VALUES (?,?,?,?,?,?)",
            (tipo, data, desc, cat, valor, sub)
        )
        n_imp += 1
    conn.commit()
    conn.close()
    partes = []
    if n_imp: partes.append(f"{n_imp} importado(s)")
    if n_dup: partes.append(f"{n_dup} duplicata(s) ignorada(s)")
    if n_err: partes.append(f"{n_err} erro(s)")
    msg = "Importação concluída: " + ", ".join(partes) + "." if partes else "Nenhum registro importado."
    cat_msg = "success" if n_imp > 0 else "warning"
    flash(msg, cat_msg)
    return redirect("/importar")


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=False)
