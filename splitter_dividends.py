import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
import requests

df = pd.read_excel('dividends.xlsx',  header=7)
df = df.iloc[1:]
df = df.drop(['FECHA ASAMBLEA', 'DESCRIPCIÓN PAGO PDU', 'MONTO TOTAL ENTREGADO\nEN DIVIDENDOS', 'FECHA INGRESO', "VALOR TOTAL DEL DIVIDENDO"], axis=1)
df = df.replace('-', np.nan)
df.dropna(subset = ["VALOR CUOTA"], inplace=True)
df = df.fillna(method='ffill', axis=0)
n = datetime.now().replace(minute=0, hour=0,second=0, microsecond=0)
df = df[df[' FECHA INICIAL'] > n]
df = df.sort_values(by=[' FECHA INICIAL', 'VALOR CUOTA'])
df['Valor Accion'] = np.nan
shares_nemo = set(df['NEMOTÉCNICO'].tolist())
for i, share in enumerate(shares_nemo):
    response = requests.post("https://www.bvc.com.co/pps/tibco/portalbvc/Home/Mercados/enlinea/acciones?com.tibco.ps.pagesvc.action=portletAction&com.tibco.ps.pagesvc.targetSubscription=5d9e2b27_11de9ed172b_-74187f000001&action=buscar", data=dict(tipoMercado=1,diaFecha=n.day,mesFecha=n.strftime("%m"),anioFecha=n.strftime("%Y"),nemo=share))  
    soup = BeautifulSoup(response.text, 'html.parser')
    value = soup.select('#texto_24 > tbody > tr:nth-child(2) > td:nth-child(5)')[0].text
    if value:
        df.loc[df['NEMOTÉCNICO'] == share,"Valor Accion"] = value.replace(".", "").replace(",", ".")

df["Valor Accion"] = pd.to_numeric(df["Valor Accion"], downcast="float")
df["value"] = df['VALOR CUOTA'] * 100 / df["Valor Accion"]
df.to_excel("analisis_{}.xls".format(n))

df = df.groupby(["NEMOTÉCNICO", "Valor Accion"])["VALOR CUOTA"].agg('sum')
#df["value"] = df['VALOR CUOTA'] * 100 / df["Valor Accion"]
df.to_excel("analisis_2-{}.xls".format(n))