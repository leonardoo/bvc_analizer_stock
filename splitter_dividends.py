import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from time import sleep

from io import BytesIO

import logging

session = requests.Session()

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

logger.info('pull xlsx for bvc')
"""
url_bvc = "https://www.bvc.com.co/pps/tibco/portalbvc/Home/Home?com.tibco.ps.pagesvc.action=updateRenderState&rp.currentDocumentID=1a8f342f_1604706963a_-27d7c0a84ca9&rp.revisionNumber=1&rp.attachmentPropertyName=Attachment&com.tibco.ps.pagesvc.targetPage=1f9a1c33_132040fa022_-78750a0a600b&com.tibco.ps.pagesvc.mode=resource&rp.redirectPage=1f9a1c33_132040fa022_-787e0a0a600b"
r = requests.get(url_bvc)
open('dividends.xlsx', 'wb').write(r.content)
logger.info('xlsx saved')

logger.info('start to process')
"""
df = pd.read_excel('dividends.xlsx',  header=7)
df = df.iloc[1:]
logger.info('start to cleanup')
df = df.drop(['FECHA ASAMBLEA', 'DESCRIPCIÓN PAGO PDU', 'MONTO TOTAL ENTREGADO\nEN DIVIDENDOS', 'FECHA INGRESO', "VALOR TOTAL DEL DIVIDENDO"], axis=1)
df = df.replace('-', np.nan)
df.dropna(subset = ["VALOR CUOTA"], inplace=True)
df = df.fillna(method='ffill', axis=0)
n = datetime.now().replace(minute=0, hour=0,second=0, microsecond=0)
df = df[df[' FECHA INICIAL'] > n]
df = df.sort_values(by=[' FECHA INICIAL', 'VALOR CUOTA'])
df['Valor Accion'] = np.nan
shares_nemo = list(set(df['NEMOTÉCNICO'].tolist()))
retries = {}
while shares_nemo:
    share = shares_nemo.pop()
    if share not in retries:
        retries[share] = 0
    retries[share] += 1
    logger.info(f'pull data for {share}')
    value = None
    data = dict(
        tipoMercado=1,
        diaFecha=n.strftime("%d"),
        mesFecha=n.strftime("%m"),
        anioFecha=n.strftime("%Y"),
        nemo=share
    )
    logger.info(f'get data: {data}')
    response = None
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
        response = session.post("https://www.bvc.com.co/pps/tibco/portalbvc/Home/Mercados/enlinea/acciones?com.tibco.ps.pagesvc.action=portletAction&com.tibco.ps.pagesvc.targetSubscription=5d9e2b27_11de9ed172b_-74187f000001&action=buscar", data=data, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        value = soup.select('#texto_24 > tbody > tr:nth-child(2) > td:nth-child(5)')[0].text
        if value:
            logger.info(f'value found for {share}: {value}')
        else:
            with open(f"acciones-{share}-{datetime.now().isoformat()}.html", "wb") as f:
                f.write(response.content)
    except Exception as e:
        with open(f"acciones-{share}-{datetime.now().isoformat()}.html", "wb") as f:
                f.write(response.content)
        logger.error(e, exc_info=True)
        if retries[share] <= 3:
            shares_nemo.append(share)
            sleep(10)
    else:
        sleep(3)

    if value:
        value = value.replace(",", "")
    logger.info(f'value for {share}: {value}')
    df.loc[df['NEMOTÉCNICO'] == share, "Valor Accion"] = value


df["Valor Accion"] = pd.to_numeric(df["Valor Accion"], downcast="float")
df["value"] = df['VALOR CUOTA'] * 100 / df["Valor Accion"]
df.to_excel(f"analisis_{n}.xls")

df = df.groupby(["NEMOTÉCNICO", "Valor Accion"])["VALOR CUOTA"].agg('sum').to_frame().reset_index()
logger.info(df.head())
df["value"] = df['VALOR CUOTA'] * 100 / df["Valor Accion"]

df.to_excel(f"analisis_2-{n}.xls")