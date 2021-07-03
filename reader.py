import locale
from datetime import datetime

import numpy as np
import pandas as pd


class Reader:

    def __init__(self, name):
        self.name = name

    def _read_excel(self):
        self.excel = pd.read_excel('dividends.xlsx',  header=7)
        self.excel = self.excel.iloc[1:]
        return self

    def _clean_excel_data(self):
        self.excel = self.excel.drop(
            ['FECHA ASAMBLEA', 'DESCRIPCIÓN PAGO PDU', 'MONTO TOTAL ENTREGADO\nEN DIVIDENDOS',
             'FECHA INGRESO', "VALOR TOTAL DEL DIVIDENDO"], axis=1)

        self.excel = self.excel.replace('-', np.nan)
        self.excel[" FECHA INICIAL"] = pd.to_datetime(self.excel[" FECHA INICIAL"], errors="coerce")
        self.excel.dropna(subset=["VALOR CUOTA", " FECHA INICIAL"], inplace=True)

        self.excel = self.excel.fillna(method='ffill', axis=0)
        date_time = datetime.now().replace(minute=0, hour=0, second=0, microsecond=0)
        self.excel = self.excel[self.excel[' FECHA INICIAL'] >= date_time]
        self.excel = self.excel.sort_values(by=[' FECHA INICIAL', 'VALOR CUOTA'])
        self.excel["VALOR CUOTA"] = self.excel["VALOR CUOTA"].astype(str).str.replace(r'(\$|\')', '').astype(float)
        self.excel['Valor Accion'] = np.nan
        return self

    @staticmethod
    def read_excel(name):
        return Reader(name)._read_excel()._clean_excel_data()

    def get_share_list(self):
        return list(set(self.excel['NEMOTÉCNICO'].tolist()))

    def set_value_share(self, share, value):
        if value:
            value = locale.atof(value)
        self.excel.loc[self.excel['NEMOTÉCNICO'] == share, "Valor Accion"] = value
