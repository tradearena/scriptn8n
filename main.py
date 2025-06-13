from fastapi import FastAPI, Request
import pandas as pd

app = FastAPI()

multiplicadores = {'WIN': 0.2, 'WDO': 10.0, 'BIT': 0.1}
emolumentos = {'WIN': 0.25, 'WDO': 1.20, 'BIT': 3.00}

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

        # Aceita tanto lista quanto dict com "orders"
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
        df['dateTime'] = pd.to_datetime(df['dateTime'], errors='coerce')
        df['side'] = df['side'].map({'0': 'COMPRA', '1': 'VENDA'})
        df['AtivoPrefixo'] = df['code'].str[:3]
        df['token'] = df['token'].astype(str)

        ultimo_preco_por_ativo = df.sort_values('dateTime').groupby('code')['price'].last().to_dict()
        resultados = []

        for usuario, grupo in df.groupby('token'):
            lucro_total = 0
            custo_total = 0
            qtd_total = 0
            ordens = len(grupo)
            qtd_compra_total = 0
            qtd_venda_total = 0

            for ativo, subgrupo in grupo.groupby('AtivoPrefixo'):
                mult = multiplicadores.get(ativo, 0)
                taxa_emol = emolumentos.get(ativo, 0)

                compras = subgrupo[subgrupo['side'] == 'COMPRA']
                vendas = subgrupo[subgrupo['side'] == 'VENDA']

                qtd_buy = compras['quantity'].sum()
                qtd_sell = vendas['quantity'].sum()
                qtd_total_ativo = subgrupo['quantity'].sum()
                qtd_base = min(qtd_buy, qtd_sell)
                qtd_aberta = abs(qtd_buy - qtd_sell)

                preco_medio_buy = (compras['quantity'] * compras['price']).sum() / qtd_buy if qtd_buy > 0 else 0
                preco_medio_sell = (vendas['quantity'] * vendas['price']).sum() / qtd_sell if qtd_sell > 0 else 0

                resultado_fechado = (preco_medio_sell - preco_medio_buy) * qtd_base * mult

                try:
                    ultimo_preco = ultimo_preco_por_ativo.get(subgrupo['code'].iloc[0], 0)
                except:
                    ultimo_preco = 0

                if qtd_buy > qtd_sell:
                    resultado_em_aberto = (ultimo_preco - preco_medio_buy) * qtd_aberta * mult
                elif qtd_sell > qtd_buy:
                    resultado_em_aberto = (preco_medio_sell - ultimo_preco) * qtd_aberta * mult
                else:
                    resultado_em_aberto = 0

                resultado_total = resultado_fechado + resultado_em_aberto
                custo_emol = qtd_total_ativo * taxa_emol

                lucro_total += resultado_total
                custo_total += custo_emol
                qtd_total += qtd_total_ativo
                qtd_compra_total += qtd_buy
                qtd_venda_total += qtd_sell

            resultados.append({
                "token": usuario,
                "lucroBruto": round(lucro_total, 2),
                "lucroLiquido": round(lucro_total - custo_total, 2),
                "qtdeOrdens": ordens,
                "qtdeContratos": qtd_total,
                "qtdeCompra": qtd_compra_total,
                "qtdeVenda": qtd_venda_total
            })

        return resultados

    except Exception as e:
        return {"erro": f"Erro interno durante o cálculo: {str(e)}"}
