import locale

from datetime import datetime
import requests
from time import sleep

from main import TriiRequest, TriiHtmlProcessor, BVCRequest
from reader import Reader, Report1, Report2, ReportApplyTrii

from logger import logger

locale.setlocale(locale.LC_ALL, 'es_CO.UTF8')

session = requests.Session()

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

an = Report1(reader.excel, f"analisis.xls").apply_operations().generate_excel()

an_trii = ReportApplyTrii(an.df, f"trii_analisis.xls").apply_operations(nemos_trii).generate_excel()
an2 = Report2(an.df, f"analisis_2.xls").apply_operations().generate_excel()
an2_trii = ReportApplyTrii(an2.df, f"trii_analisis_2.xls").apply_operations(nemos_trii).generate_excel()
