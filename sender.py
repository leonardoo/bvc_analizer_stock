import datetime
import requests

from reader import ReaderSender

reader = ReaderSender.read_excel("2021-07-09/analisis.xls")

def convert_to_dict(data):
    return {
        "name": data["EMISOR"],
        "nemo": data["NEMOTÉCNICO"],
        "day": datetime.date.today().isoformat(),
        "value": data["Valor Accion"],
        "currency": data["MONEDA"],
        "total": data["VALOR TOTAL DEL DIVIDENDO"],
        "paid_amount": data["VALOR CUOTA"],
        "ex_dividend_date": data[" FECHA INICIAL"].date().isoformat(),
        "paid_at": data[" FECHA FINAL\nY DE PAGO"].date().isoformat()
    }

print(reader.excel.head())
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiZTExOWZhZTYtNzZlZS00NmFiLThkZDItOGIwOWZjMTE3MzI5IiwiYXVkIjoiZmFzdGFwaS11c2VyczphdXRoIiwiZXhwIjoxNjI1ODM0NzUzfQ.fuaF8WSDpJEMjARVnezJcyQrfFM8IseruakwRqw86Do"
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': "application/json",
}
for label, content in reader.excel.iterrows():
    print(label, content)
    data = convert_to_dict(content)
    response = requests.post(
        "http://localhost:8000/stock/data/",
        json=data,
        headers=headers
    )
    response.raise_for_status()
