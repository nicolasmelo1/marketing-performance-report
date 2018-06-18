from googleads import adwords
from facebook_business.api import FacebookAdsApi
from requests_oauthlib import OAuth1
from pydrive.auth import GoogleAuth
import psycopg2
import os


# Initialization class, used to initialize stuff
def googledriveinit():
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile('/'.join(os.path.realpath(__file__).replace('\\', '/').split('/')[:-2])
                              + '/' + "credentials.json")
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        try:
            gauth.Refresh()
        except Exception as e:
            print('[Exception] - Exception fired on Google Drive Init:' + str(e))
            gauth.LocalWebserverAuth()
    else:
        gauth.Authorize()

    gauth.SaveCredentialsFile('/'.join(os.path.realpath(__file__).replace('\\', '/').split('/')[:-2])
                              + '/' + "credentials.json")
    return gauth


# Initialize Twitter
def twitterinit():
    consumer_key = 'consumer_key'
    consumer_secret = 'consumer_secret'
    access_token = 'access_token'
    access_token_secret = 'access_token_secret'

    auth = OAuth1(consumer_key, consumer_secret,
                  access_token, access_token_secret)
    return auth


# Initialize Facebook
def facebookinit():
    my_app_id = 'app_id'
    my_app_secret = 'app_secret'
    my_access_token = 'access_token'

    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)


# Initialize Adwords
# the client_customer_id is set to drivers account
def adwordsinit(customerId=None):
    adwords_client_string='''adwords:
  developer_token: developer_token
  client_customer_id: client_customer_id
  client_id: client_id
  client_secret: client_secret
  refresh_token: refresh_token'''
    adwords_client = adwords.AdWordsClient.LoadFromString(adwords_client_string)
    if customerId:
        adwords_client.SetClientCustomerId(customerId)
    return adwords_client


# Initialize Appsflyer
def appsflyerinit():
    return 'appsflyer_acces_token'


# Initialize Database
def databseinit():
    try:
        conn = psycopg2.connect(dbname='db_name', user='user',
                                host='host', password='password',
                                port='port')
        return conn
    except:
        print("I am unable to connect to the database")
        return None
