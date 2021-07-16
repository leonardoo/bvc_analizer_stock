import locale
from abc import ABC, abstractmethod
from datetime import datetime, date
from pathlib import Path

import numpy as np
import pandas as pd


class ReaderBase:
    def __init__(self, name):
        self.name = name
        self.header = 0

    def _read_excel(self):
        self.excel = pd.read_excel(self.name, engine='openpyxl', header=self.header)
        return self

    def _clean_excel_data(self):
        return self

    @classmethod
    def read_excel(cls, name):
        return cls(name)._read_excel()._clean_excel_data()


class ReaderSender(ReaderBase):
    def _read_excel(self):
        super()._read_excel()
        self.excel.dropna(subset=["Valor Accion"], inplace=True)
        return self


class Reader(ReaderBase):

    def __init__(self, name):
        super().__init__(name)
        self.header = 7

    def _read_excel(self):
        super()._read_excel()
        self.excel = self.excel.iloc[1:]
        return self

    def _clean_excel_data(self):
        self.excel = self.excel.drop(
            ['FECHA ASAMBLEA', 'DESCRIPCIÓN PAGO PDU', 'MONTO TOTAL ENTREGADO\nEN DIVIDENDOS',
             'FECHA INGRESO'],
            axis=1
        )

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

    def get_share_list(self):
        return list(set(self.excel['NEMOTÉCNICO'].tolist()))

    def set_value_share(self, share, value):
        if value:
            value = locale.atof(value)
        self.excel.loc[self.excel['NEMOTÉCNICO'] == share, "Valor Accion"] = value


class GenerateReport(ABC):

    def __init__(self, dataframe, file_name):
        self.df = dataframe
        self.file_name = file_name

    @abstractmethod
    def apply_operations(self, **kwargs):
        pass

    def generate_excel(self):
        self._get_file_path()
        self.df.to_excel(self.file_name, engine='openpyxl')
        return self

    def _get_file_path(self):
        path = Path(__file__).absolute().parent
        path = path.joinpath(f"{date.today()}/")
        path.mkdir(parents=True, exist_ok=True)
        self.file_name = str(path.joinpath(self.file_name))


class Report1(GenerateReport):

    def apply_operations(self, **kwargs):
        self.df["Valor Accion"] = pd.to_numeric(self.df["Valor Accion"], downcast="float")
        self.df["value"] = self.df['VALOR CUOTA'] * 100 / self.df["Valor Accion"]
        self.df = self.df.loc[:, ~self.df.columns.str.contains('^Unnamed')]
        return self


class ReportApplyTrii(GenerateReport):

    def apply_operations(self, nemos_trii, **kwargs):
        self.df = self.df[self.df['NEMOTÉCNICO'].isin(nemos_trii)]
        return self


class Report2(GenerateReport):

    def apply_operations(self, **kwargs):
        self.df = self.df.groupby(["NEMOTÉCNICO", "Valor Accion"])["VALOR CUOTA"].agg('sum').to_frame().reset_index()
        self.df["value"] = self.df['VALOR CUOTA'] * 100 / self.df["Valor Accion"]
        return self
