from fastapi import FastAPI, Request
import pandas as pd
import traceback
import numpy as np

app = FastAPI()

multiplicadores = {'WIN': 0.2, 'WDO': 10.0, 'BIT': 0.1}
emolumentos = {'WIN': 0.25, 'WDO': 1.20, 'BIT': 3.00}

def convert_numpy(obj):
    if isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    return obj

def map_side(val):
    if val is None:
        return np.nan
    s = str(val).strip().upper()
    if s in {"0", "BUY", "COMPRA", "B"}:
        return "COMPRA"
    if s in {"1", "SELL", "VENDA", "S"}:
        return "VENDA"
    return np.nan

@app.get("/")
def ping():
    return {"mensagem": "API ativa. Use POST / ou /calcular-resultado"}

@app.post("/")
async def calcular_raiz(request: Request):
    return await calcular_resultado(request)

@app.post("/calcular-resultado")
async def calcular_resultado(request: Request):
    try:
        json_data = await request.json()
        if isinstance(json_data, list):
            orders = json_data
        elif isinstance(json_data, dict) and "orders" in json_data:
            orders = json_data["orders"]
        else:
            return {"erro": "Formato de JSON inválido. Envie um array ou um objeto com 'orders'."}
        if not orders:
            return {"erro": "Lista de ordens vazia."}
    except Exception as e:
        return {"erro": f"Erro ao processar JSON: {str(e)}"}

    try:
        df = pd.DataFrame(orders)

        # Normalizações
        df["dateTime"] = pd.to_datetime(df["dateTime"], errors="coerce")
        df["side"] = df["side"].apply(map_side)
        df["code"] = df["code"].astype(str).str.upper()
        df["AtivoPrefixo"] = df["code"].str[:3]
        df["token"] = df["token"].astype(str)
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

        df = df.dropna(subset=["quantity", "price", "dateTime", "side", "code"])

        # Último preço GLOBAL por code (entre tokens), para marcar a ordem fantasma
        ultimo_preco_por_code = (
            df.sort_values("dateTime")
              .groupby("code")["price"]
              .last()
              .to_dict()
        )

        resultados = []

        for usuario, g_token in df.groupby("token"):
            lucro_realizado_total = 0.0
            pnl_aberto_total = 0.0
            custo_total = 0.0

            qtde_contratos_total = 0.0
            qtde_compra_total = 0.0
            qtde_venda_total = 0.0
            ordens = len(g_token)

            # Processa por CODE (não só prefixo)
            for code, g_code in g_token.groupby("code"):
                prefixo = code[:3]
                mult = multiplicadores.get(prefixo, 0.0)
                taxa_emol = emolumentos.get(prefixo, 0.0)

                if mult == 0.0:
                    # Se não mapeado, considera sem PnL (pode logar/avisar fora)
                    pass

                g_code = g_code.sort_values("dateTime")

                # Totais para métricas
                qtde_contratos_total += g_code["quantity"].sum()
                qtde_compra_total += g_code.loc[g_code["side"] == "COMPRA", "quantity"].sum()
                qtde_venda_total += g_code.loc[g_code["side"] == "VENDA", "quantity"].sum()

                # Emolumentos proporcionais ao volume negociado desse code
                custo_total += g_code["quantity"].sum() * taxa_emol

                # ____ Loop WAC (custo médio móvel) para posição e PnL realizado ____
                pos = 0.0            # >0 long, <0 short, 0 zerado
                pm = 0.0             # preço médio da posição aberta (sempre positivo)
                pnl_realizado = 0.0  # acumulado

                for _, row in g_code.iterrows():
                    qty = float(row["quantity"])
                    px = float(row["price"])
                    side = row["side"]

                    if side == "COMPRA":
                        # Fecha short se existir
                        if pos < 0:
                            match_qty = min(qty, -pos)
                            # Short fecha comprando: ganho = (pm - px) * qtd
                            pnl_realizado += (pm - px) * match_qty * mult
                            pos += match_qty
                            qty -= match_qty
                            if pos == 0:
                                pm = 0.0
                        # Abre/aumenta long com eventual sobra
                        if qty > 0:
                            # se estava zerado, pm vira px
                            if pos == 0:
                                pm = px
                                pos = qty
                            else:
                                # pos >= 0 aqui
                                pm = (pm * pos + px * qty) / (pos + qty)
                                pos += qty

                    elif side == "VENDA":
                        # Fecha long se existir
                        if pos > 0:
                            match_qty = min(qty, pos)
                            # Long fecha vendendo: ganho = (px - pm) * qtd
                            pnl_realizado += (px - pm) * match_qty * mult
                            pos -= match_qty
                            qty -= match_qty
                            if pos == 0:
                                pm = 0.0
                        # Abre/aumenta short com eventual sobra
                        if qty > 0:
                            # se estava zerado, pm vira px
                            if pos == 0:
                                pm = px
                                pos = -qty
                            else:
                                # pos <= 0 aqui
                                abspos = abs(pos)
                                pm = (pm * abspos + px * qty) / (abspos + qty)
                                pos -= qty
                    else:
                        # side inválido (já filtrado), mas por segurança
                        continue

                # ____ PnL aberto via "ordem fantasma" (ultima cotação) ____
                ultimo_preco = ultimo_preco_por_code.get(code, g_code["price"].iloc[-1])

                pnl_aberto = 0.0
                if pos > 0:
                    # Long aberto: vender ao último preço
                    pnl_aberto = (ultimo_preco - pm) * pos * mult
                elif pos < 0:
                    # Short aberto: recomprar ao último preço
                    pnl_aberto = (pm - ultimo_preco) * (-pos) * mult

                lucro_realizado_total += pnl_realizado
                pnl_aberto_total += pnl_aberto

            lucro_bruto = lucro_realizado_total + pnl_aberto_total
            lucro_liquido = lucro_bruto - custo_total

            resultados.append({
                "token": usuario,
                "lucroBruto": round(lucro_bruto, 2),
                "lucroLiquido": round(lucro_liquido, 2),
                "qtdeOrdens": ordens,
                "qtdeContratos": round(qtde_contratos_total, 2),
                "qtdeCompra": round(qtde_compra_total, 2),
                "qtdeVenda": round(qtde_venda_total, 2)
            })

        # Conversão p/ tipos nativos
        resultados = [
            {k: convert_numpy(v) for k, v in item.items()}
            for item in resultados
        ]
        return resultados

    except Exception as e:
        print(traceback.format_exc())
        return {"erro": f"Erro interno durante o cálculo: {str(e)}"}
