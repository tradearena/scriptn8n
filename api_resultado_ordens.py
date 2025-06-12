# By Jarbas - API FastAPI para calcular resultado de ordens
from fastapi import FastAPI
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

@app.post("/calcular")
async def calcular_ordens(ordens: List[Ordem]):
    df = pd.DataFrame([o.dict() for o in ordens])

    df['Lado'] = df['side'].astype(str).map({'1': 'COMPRA', '2': 'VENDA'}).fillna(df['side'])
    df['Data de Fechamento'] = pd.to_datetime(df['dateTime'])
    df['Conta de Roteamento'] = df['token'].astype(str)
    df['Ativo'] = df['code'].str.strip()
    df['AtivoPrefixo'] = df['Ativo'].str[:3]
    df['Preço Médio'] = df['price'] / 100.0
    df['Quantidade Executada'] = df['quantity'].astype(float)

    multiplicadores = {'WIN': 0.2, 'WDO': 10.0, 'BIT': 0.1}
    emolumentos = {'WIN': 0.25, 'WDO': 1.20, 'BIT': 3.00}

    resultados = []
    agrupado = df.groupby(['Conta de Roteamento', 'AtivoPrefixo'])

    for (usuario, ativo), grupo in agrupado:
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

        resultados.append({
            'token': usuario,
            'ativo': ativo,
            'contratos': int(qtd_total),
            'preco_medio_compra': round(preco_medio_buy, 2),
            'preco_medio_venda': round(preco_medio_sell, 2),
            'resultado_fechado': round(resultado_fechado, 2),
            'resultado_aberto': round(resultado_em_aberto, 2),
            'resultado_total': round(resultado_total, 2),
            'custo_emol': round(custo_emolumento, 2),
            'resultado_liquido': round(resultado_total - custo_emolumento, 2),
            'qtd_compra': int(qtd_buy),
            'qtd_venda': int(qtd_sell),
            'em_aberto': 'SIM' if qtd_buy != qtd_sell else 'NÃO'
        })

    return resultados
