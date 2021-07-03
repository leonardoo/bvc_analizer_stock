import locale

import pandas as pd

from datetime import datetime
import requests
from time import sleep


import logging

from main import TriiRequest, TriiHtmlProcessor, BVCRequest
from reader import Reader

locale.setlocale(locale.LC_ALL, 'es_CO.UTF8')

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

url_bvc = "https://www.bvc.com.co/pps/tibco/portalbvc/Home/Home?com.tibco.ps.pagesvc.action=updateRenderState&rp.currentDocumentID=1a8f342f_1604706963a_-27d7c0a84ca9&rp.revisionNumber=1&rp.attachmentPropertyName=Attachment&com.tibco.ps.pagesvc.targetPage=1f9a1c33_132040fa022_-78750a0a600b&com.tibco.ps.pagesvc.mode=resource&rp.redirectPage=1f9a1c33_132040fa022_-787e0a0a600b"
r = requests.get(url_bvc)
with open('dividends.xlsx', 'wb') as fi: 
    fi.write(r.content)
logger.info('xlsx saved')

trii_request = TriiRequest.request()
nemos_trii = []
try:
    nemos_trii = TriiHtmlProcessor.process(trii_request.soup)
except Exception as e:
    logger.exception("a")

logger.info('start to process')
logger.info('read and cleanup')
reader = Reader.read_excel("dividends.xlsx")
shares_nemo = reader.get_share_list()
retries = {}
n = datetime.now()
while shares_nemo:
    share = shares_nemo.pop()
    if share not in retries:
        retries[share] = 0
    retries[share] += 1
    logger.info(f'pull data for {share}')
    value = None
    response = None
    try:
        request = BVCRequest.request(nemo=share)
        response = request.response
        soup = request.soup
        value = soup.select('#texto_24 > tbody > tr:nth-child(2) > td:nth-child(5)')[0].text
        if value:
            logger.info(f'value found for {share}: {value}')
        else:
            with open(f"acciones-{share}-{datetime.now().isoformat()}.html", "wb") as f:
                f.write(request.response.content)
    except Exception as e:
        logger.error(e, exc_info=True)

        try:
            with open(f"acciones-{share}-{datetime.now().isoformat()}.html", "wb") as f:
                f.write(response.content)
        except Exception as e:
            pass
        if retries[share] <= 3:
            shares_nemo.append(share)
            sleep(10)
    else:
        sleep(4)
    logger.info(f'value for {share}: {value}')
    reader.set_value_share(share, value)

df = reader.excel
df["Valor Accion"] = pd.to_numeric(df["Valor Accion"], downcast="float")
df["value"] = df['VALOR CUOTA'] * 100 / df["Valor Accion"]
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df_base = df
df_base = df_base[df_base['NEMOTÉCNICO'].isin(nemos_trii)]
df_base.to_excel(f"trii_analisis_{n}_trii.xls")
df.to_excel(f"analisis_{n}.xls")

df = df.groupby(["NEMOTÉCNICO", "Valor Accion"])["VALOR CUOTA"].agg('sum').to_frame().reset_index()
logger.info(df.head())
df["value"] = df['VALOR CUOTA'] * 100 / df["Valor Accion"]
df_base = df[df['NEMOTÉCNICO'].isin(nemos_trii)]
df_base.to_excel(f"trii_analisis_2-{n}.xls")
df.to_excel(f"analisis_2-{n}.xls")