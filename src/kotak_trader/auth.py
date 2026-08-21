from time import time
import json
from pathlib import Path

from neo_api_client import NeoAPI
import os
import pyotp
import pandas as pd


# Activate logging for debugging purposes. Uncomment the following lines to enable logging.
# import logging

# logging.basicConfig(
#     level=logging.DEBUG,
#     format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
# )


# access_token: It is optional. 
# environment: You pass prod to connect to live server
# neo_fin_key: It is optional. Pass None.
# consumer_key: this is the token that is available on your NEO app or website.
# To get consumer key, login to kotak NEO app or web -> invest tab -> trade api card. Generate application.
# with default application, you will have a copyable token. Pass this token in consumer_key.

consumer_key = os.getenv("KOTAK_CONSUMER_KEY")
mpin = os.getenv("KOTAK_MPIN")
mobile = os.getenv("KOTAK_MOBILE_NUMBER")
client_code = os.getenv("KOTAK_UCC")
totp_secret = os.getenv("KOTAK_TOTP_SECRET")
# TOKEN_FILE = Path(".tokens/session.json")

# def load_token():
#     if not TOKEN_FILE.exists():
#         return None

#     data = json.loads(TOKEN_FILE.read_text())

#     # if time.time() >= data["expires_at"]:
#     #     return None

#     return data["access_token"]

# def save_token(access_token):
#     TOKEN_FILE.parent.mkdir(exist_ok=True)

#     data = {
#         "access_token": access_token,
#         # "expires_at": time.time() + expires_in
#     }

#     TOKEN_FILE.write_text(json.dumps(data))


# # Login using TOTP

# access_token = load_token()

def get_authenticated_client():
    print("Authenticating...")
    client = NeoAPI(environment='prod', consumer_key=consumer_key)

    try:
        totp = pyotp.TOTP(totp_secret)
        otp = totp.now()

        response = client.totp_login(mobile_number=mobile, ucc=client_code, totp=otp)
        client.totp_validate(mpin=mpin)
        return client
        print("Authenticated successfully.")
    except Exception as e:
        print("Exception:", type(e).__name__)
        print("Message:", str(e))




# print("access_token:", response['data']['token'], 
#       'sid:', response['data']['sid'],
#       "datacenter:", response['data']['dataCenter'])

# trade_response = client.trade_report()
# trade_df = pd.DataFrame(trade_response['data'])
# trade_df.to_csv('outputs/trade_report.csv', index=False)

# holding_response = client.holdings()
# holding_df = pd.DataFrame(holding_response['data'])
# holding_df.to_csv('outputs/holding_report.csv', index=False)

# position_response = client.positions()
# position_df = pd.DataFrame(position_response['data'])
# position_df.to_csv('outputs/position_report.csv', index=False)

# client.logout()

