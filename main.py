import locale
from abc import abstractmethod, ABC

from requests import Request, Session
from bs4 import BeautifulSoup
from datetime import datetime


import logging
logger = logging.getLogger(__file__)


class RequestPage(ABC):

    session = Session()
    url = None
    requester = None

    def _requests(self, pre_request):
        request = self.session.prepare_request(pre_request)
        self.response = self.session.send(request)
        self.response.raise_for_status()
        self.soup = BeautifulSoup(self.response.text, 'html.parser')
        return self

    @classmethod
    def request(cls, **kwargs):
        if not isinstance(cls.requester, cls):
            cls.requester = cls()
        request = cls.requester._generate_requests(**kwargs)
        cls.requester._requests(request)
        return cls.requester

    @abstractmethod
    def _generate_requests(self, **kwargs):
        pass


class TriiRequest(RequestPage):
    url = "https://www.trii.co/stock-list"

    def _generate_requests(self, **kwargs):
        return Request('GET', self.url)


class BVCRequest(RequestPage):
    url = "https://www.bvc.com.co/pps/tibco/portalbvc/Home/Mercados/enlinea/acciones?com.tibco.ps.pagesvc.action=portletAction&com.tibco.ps.pagesvc.targetSubscription=5d9e2b27_11de9ed172b_-74187f000001&action=buscar"
    data = None

    def __init__(self, *args, **kwargs):
        date = datetime.now().replace(minute=0, hour=0,second=0, microsecond=0)
        self.data = dict(
            tipoMercado=1,
            diaFecha=date.strftime("%d"),
            mesFecha=date.strftime("%m"),
            anioFecha=date.strftime("%Y"),
            nemo=None
        )

    def _generate_requests(self, nemo, **kwargs):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
        data = {**self.data, **{"nemo": nemo}}
        logger.info(f'request with: {data}')
        return Request('POST', self.url, data=data, headers=headers)
        

class TriiHtmlProcessor:

    selector = "#actions > a> div.d-flex.align-items-center > div.action__info > h4"

    @classmethod
    def process(cls, soup):
        return [element.text.strip()
                for element in soup.select(cls.selector)]


class BVCHtmlProcessor:
    selector = '#texto_24 > tbody > tr:nth-child(2) > td:nth-child(5)'

    def process(self, soup):
        value = soup.select(self.selector)[0].text
        if value:
            value = locale.atof(value)
        return value