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

client = NeoAPI(environment='prod', consumer_key=consumer_key)

# Login using TOTP

# Complete your TOTP registration from Kotak Securities website. Follow steps mentioned below.

# Visit https://www.kotaksecurities.com/platform/kotak-neo-trade-api/ and select Register for Totp.
# Totp registration is a one time step where you can register for totp on your mobile and start receiving totps.

# Step 1 - Verify your mobile no with OTP

# Step 2 - Select account, for which you want to register for totp

# Step 3 - Select option to register for totp

# Step 4 - You will receive a QR code, which is valid for 5 minutes

# Step 5 - Open any authenticator app, and scan the QR code

# Step 6 - You will start receiving the Totps on the authenticator apps

# Step 7 - Submit the totp on the QR code page to complete the Totp registration

# mobile_number: registered mobile number with the country code.
# ucc: Unique Client Code which you will find in mobile application/website under profile section
# totp: Time-based One-Time Password recieved on google authenticator application
# totp_login generates the view token and session id used to generate trade token
# totp_validate generates the trade token
# # # mpin: mpin for your neo account

totp = pyotp.TOTP(totp_secret)
otp = totp.now()

print("TOTP Length:", len(otp))

try:
    response = client.totp_login(mobile_number=mobile, ucc=client_code, totp=otp)
    # print("Response:", response)
except Exception as e:
    print("Exception:", type(e).__name__)
    print("Message:", str(e))

try: 
    client.totp_validate(mpin=mpin)
except Exception as e:
    print("Exception when validating TOTPLogin ->login: %s\n" % e)    

trade_response = client.trade_report()
trade_df = pd.DataFrame(trade_response['data'])
trade_df.to_csv('trade_report.csv', index=False)

client.logout()

