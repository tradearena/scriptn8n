# By Jarbas - API FastAPI para calcular resultado de ordens (agrupado por usuário)
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import pandas as pd
from datetime import datetime

app = FastAPI()

class Ordem(BaseModel):
    code: str
    side: str  # '1' para compra, '2' para venda
    dateTime: str
    price: float
    tradeId: str
    groupOrderId: str
    quantity: int
    token: int

@app.get("/")
def raiz():
    return {"mensagem": "🚀 API ativa! Use POST / para calcular ordens agrupadas por usuário."}

@app.post("/")
async def calcular_ordens(ordens: List[Ordem]):
    if not ordens:
        raise HTTPException(status_code=400, detail="Lista de ordens vazia.")

    try:
        df = pd.DataFrame([o.dict() for o in ordens])
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Erro ao converter ordens em DataFrame: {e}")

    try:
        df['Lado'] = df['side'].astype(str).map({'0': 'COMPRA', '1': 'VENDA'}).fillna(df['side'])
        df['Data de Fechamento'] = pd.to_datetime(df['dateTime'])
        df['Conta de Roteamento'] = df['token'].astype(str)
        df['Ativo'] = df['code'].str.strip()
        df['AtivoPrefixo'] = df['Ativo'].str[:3]
        df['Preço Médio'] = df['price'] / 100.0
        df['Quantidade Executada'] = df['quantity'].astype(float)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Erro ao processar os dados das ordens: {e}")

    multiplicadores = {'WIN': 0.2, 'WDO': 10.0, 'BIT': 0.1}
    emolumentos = {'WIN': 0.25, 'WDO': 1.20, 'BIT': 3.00}

    resultados = []
    try:
        for usuario, grupo_usuario in df.groupby('Conta de Roteamento'):
            total_contratos = 0
            total_resultado_fechado = 0
            total_resultado_aberto = 0
            total_resultado_total = 0
            total_emolumentos = 0
            total_qtd_compra = 0
            total_qtd_venda = 0
            total_ordens = len(grupo_usuario)

            ativos = grupo_usuario['AtivoPrefixo'].unique()
            for ativo in ativos:
                grupo = grupo_usuario[grupo_usuario['AtivoPrefixo'] == ativo]
                mult = multiplicadores.get(ativo, 0)
                taxa_emol = emolumentos.get(ativo, 0)

                compras = grupo[grupo['Lado'] == 'COMPRA']
                vendas = grupo[grupo['Lado'] == 'VENDA']

                qtd_buy = compras['Quantidade Executada'].sum()
                qtd_sell = vendas['Quantidade Executada'].sum()
                qtd_total = grupo['Quantidade Executada'].sum()
                qtd_base = min(qtd_buy, qtd_sell)
                qtd_aberta = abs(qtd_buy - qtd_sell)

                preco_medio_buy = (compras['Quantidade Executada'] * compras['Preço Médio']).sum() / qtd_buy if qtd_buy > 0 else 0
                preco_medio_sell = (vendas['Quantidade Executada'] * vendas['Preço Médio']).sum() / qtd_sell if qtd_sell > 0 else 0

                resultado_fechado = (preco_medio_sell - preco_medio_buy) * qtd_base * mult
                ultimo_preco = grupo.sort_values('Data de Fechamento')['Preço Médio'].iloc[-1]

                if qtd_buy > qtd_sell:
                    resultado_em_aberto = (ultimo_preco - preco_medio_buy) * qtd_aberta * mult
                elif qtd_sell > qtd_buy:
                    resultado_em_aberto = (preco_medio_sell - ultimo_preco) * qtd_aberta * mult
                else:
                    resultado_em_aberto = 0

                resultado_total = resultado_fechado + resultado_em_aberto
                custo_emolumento = qtd_total * taxa_emol

                total_contratos += qtd_total
                total_resultado_fechado += resultado_fechado
                total_resultado_aberto += resultado_em_aberto
                total_resultado_total += resultado_total
                total_emolumentos += custo_emolumento
                total_qtd_compra += qtd_buy
                total_qtd_venda += qtd_sell

            resultados.append({
                'token': usuario,
                'ordens': int(total_ordens),
                'contratos': int(total_contratos),
                'resultado_fechado': round(total_resultado_fechado, 2),
                'resultado_aberto': round(total_resultado_aberto, 2),
                'resultado_total': round(total_resultado_total, 2),
                'custo_emol': round(total_emolumentos, 2),
                'resultado_liquido': round(total_resultado_total - total_emolumentos, 2),
                'qtd_compra': int(total_qtd_compra),
                'qtd_venda': int(total_qtd_venda),
                'em_aberto': 'SIM' if total_qtd_compra != total_qtd_venda else 'NÃO'
            })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro durante o cálculo dos resultados: {e}")

    return resultados
