import random
import datetime

from publisher import RedPandaPublisher
from config import Config

config = Config()
print(config)

redpanda = RedPandaPublisher(config)

nubank_categories = {
    "eletr\u00f4nicos": {
        "probability": 7,
        "mean": 400,
        "deviation": 300,
    },
    "mercado": {
        "probability": 23,
        "mean": 200,
        "deviation": 100,
    },
    "restaurante": {
        "probability": 25,
        "mean": 80,
        "deviation": 50,
    },
    "lazer": {
        "probability": 10,
        "mean": 100,
        "deviation": 50,
    },
    "transporte": {
        "probability": 60,
        "mean": 15,
        "deviation": 7,
    },
    "bar": {
        "probability": 30,
        "mean": 70,
        "deviation": 40,
    }
}

def generate_mock_body(amount, category, time, lat=-23.6282212, lon=-46.6619152, description="MockEntry") -> dict:
    formated_time = datetime.datetime.strftime(time, "%Y-%m-%dT%H:%M:%S") + ".000Z"
    return {
        "description": description,
        "category": "transaction",
        "amount": amount,
        "time": formated_time,
        "source": "upfront_national",
        "title": category,
        "amount_without_iof": amount,
        "account": "5a424050-fd44-4930-8787-14e94d89df5f",
        "details": {
            "status": "settled",
            "lat": lat,
            "lon": lon,
            "subcategory": "card_present"
        },
        "id": "62519e7c-f4ee-4c91-af57-ecc21d211469",
        "_links": {
            "self": {
                "href": "https://prod-s3-facade.nubank.com.br/api/transactions/62519e7c-f4ee-4c91-af57-ecc21d211469"
            }
        },
        "tokenized": False,
        "href": "nuapp://transaction/62519e7c-f4ee-4c91-af57-ecc21d211469"
    }

class MonthlyInvoices:
    def __init__(self) -> None:
        self.monthly_invoices = {}
        pass

    def add(self, time: datetime, value):
        key = f"{time.year}_{time.month}"
        if key not in self.monthly_invoices:
            self.monthly_invoices[key] = 0
        self.monthly_invoices[key] += value

    def get(self, month, year):
        key = f"{year}_{month}"
        if key in self.monthly_invoices:
            return self.monthly_invoices[key]
        return 0

def generate_card_transactions(stats: dict) -> list:
    transactions = []
    start_date = datetime.datetime(year=2020, month=1, day=1, hour=10, minute=20, second=30)
    final_date = datetime.datetime(year=2023, month=9, day=1, hour=10, minute=20, second=30)

    current_date = start_date
    while current_date < final_date:
        for category in stats:
            rand = random.randint(1, 50)
            if rand <= stats[category]["probability"]:
                mu = stats[category]["mean"]
                sigma = stats[category]["deviation"]
                value = int(random.gauss(mu, sigma) * 100)
                transactions += [generate_mock_body(value, category, current_date)]
                invoices.add(current_date, value)

        current_date += datetime.timedelta(days=1)
    return transactions


airport_geo = {
    "POA": {
        "lat": -29.9994122,
        "lon": -51.2025821
    },
    "CGH": {
        "lat": -23.6282212,
        "lon": -46.6619152
    },
    "GIG": {
        "lat": -22.80526,
        "lon": -43.2592042
    },
    "FOR": {
        "lat": -3.7708695,
        "lon": -38.5430269
    }
}

def generate_geo_fraud_transactions(airportA, airportB, timedelta_hours):
    first_date = datetime.datetime(year=2020, month=1, day=1, hour=10, minute=20, second=30)
    transactions = []
    transactions += [generate_mock_body(
        100,
        "mercado",
        first_date,
        lat=airport_geo[airportA]["lat"],
        lon=airport_geo[airportA]["lon"],
        description="Padaria do seu zé"
    )]
    second_date = first_date + datetime.timedelta(hours=timedelta_hours)
    transactions += [generate_mock_body(
        100,
        "electronic",
        second_date,
        lat=airport_geo[airportB]["lat"],
        lon=airport_geo[airportB]["lon"],
        description="Operação maliciosa"
    )]
    return transactions

nubank_account = {
    "aluguel": {
        "probability": 100,
        "value": -1500
    },
    "psicologo": {
        "probability": 100,
        "value": -500
    },
    "contas": {
        "probability": 100,
        "mean": -400,
        "deviation": 100,
    },
    "viagens_longas": {
        "probability": 15,
        "mean": -4000,
        "deviation": 2000,
    },
    "gadgets": {
        "probability": 50,
        "mean": -700,
        "deviation": 400,
    },
    "gadgets": {
        "probability": 100,
        "mean": 6000,
        "deviation": 600,
    },
}

def generate_mock_account_entry(value, name, date) -> dict:
    return {
        "tags": [
            "money-in"
        ],
        "showClock": False,
        "displayDate": "Hoje",
        "footer": "Pix",
        "title": name,
        "detailsDeeplink": "nuapp://flutter/savings/details-screen/type/TRANSFER_IN/id/640ce9bf-a77a-46ac-a6d9-266bda1c5204",
        "id": "640ce9bf-a77a-46ac-a6d9-266bda1c5204",
        "strikethrough": False,
        "kind": "POSITIVE",
        "iconKey": "nuds_v2_icon.money_in",
        "postDate": datetime.datetime.strftime(date, "%Y-%m-%d"),
        "detail": name,
        "amount": value
    }

def generate_account_transactions(info: dict) -> list:
    transactions = []
    start_date = datetime.datetime(year=2020, month=1, day=1, hour=10, minute=20, second=30)
    final_date = datetime.datetime(year=2023, month=9, day=1, hour=10, minute=20, second=30)

    current_date = start_date
    while current_date < final_date:
        for name in info:
            rand = random.randint(1, 100)
            if rand <= info[name]["probability"]:
                if "value" in info[name]:
                    value = info[name]["value"]
                else:
                    mu = info[name]["mean"]
                    sigma = info[name]["deviation"]
                    value = int(random.gauss(mu, sigma) * 100)
                transactions += [generate_mock_account_entry(value, name, current_date)]
        value = invoices.get(current_date.month, current_date.year)
        transactions += [generate_mock_account_entry(value, "invoices", current_date)]
        current_date += datetime.timedelta(days=31)
    return transactions

invoices = MonthlyInvoices()
entries = generate_card_transactions(nubank_categories)
entries += generate_geo_fraud_transactions("CGH", "FOR", 1)

for entry in entries:
    print(entry)
    redpanda.topic = "nubank.card.entry"
    redpanda.publish(entry)

account_entries = generate_account_transactions(nubank_account)

for entry in account_entries:
    redpanda.topic = "nubank.account.entry"
    redpanda.publish(entry)
