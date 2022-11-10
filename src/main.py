from pynubank import Nubank
import os
import json

cpf = os.environ['FINANCES_CPF']
password = os.environ['FINANCES_PASSWORD']
cert_file = os.environ['FINANCES_CERT_FILE']

nu = Nubank()
nu.authenticate_with_cert(cpf, password, cert_file)

card_statements = nu.get_card_statements()
with open("card_statements.json", "w+") as fo:
	json.dump(card_statements, fo)

account_statements = nu.get_account_statements()
with open("account_statements.json", "w+") as fo:
	json.dump(account_statements, fo)
