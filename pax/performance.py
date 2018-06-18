from pax.appsflyer import appsflyerData
from pax.facebook import facebookdata
from pax.google import googlereports
from pax.database import baseDatabase
from utils.paths import PATH_SIGLAS_PRACAS
from pax.twitter import twitterdata
import unidecode
import pandas
import utils.time
pandas.options.mode.chained_assignment = None


def cleanNewAppData(performance):

    performance['os_name'][performance['campaign'].str.contains('iOS', na=False)] = 'ios'
    performance['os_name'][performance['campaign'].str.contains('IOS', na=False)] = 'ios'

    performance['campaign'][performance['campaign'] == 'GDN_And_MKT_BH-CPI924382686'] = 'GDN_And_MKT_BH-CPI'
    performance['campaign'][performance['campaign'] == 'GDN_And_MKT_VIX-CPC931642203'] = 'GDN_And_MKT_VIX-CPC'
    performance['campaign'][performance['campaign'] == 'GDN_And_MKT_VIX-CPI930974807'] = 'GDN_And_MKT_VIX-CPI'
    performance['campaign'][performance['campaign'] == 'SMS_All_Ops_CWB/market://details?id=com.app99.pax'] = 'SMS_All_Ops_CWB'
    performance['campaign'][performance['campaign'] == 'Spotify_All_Brand_BH-overlay'] = 'Spotify_All_Brand_BH'
    performance['campaign'][performance['campaign'] == 'SRC_And_Conc-GYN_UA929042102'] = 'SRC_And_Conc-GYN_UA'

    performance['campaign'][performance['campaign'] == 'RG-CWB-IOS-AppInstal'] = 'RG-CWB-IOS-AppInstall'

    if 'region' in performance.columns:
        region = pandas.read_csv(PATH_SIGLAS_PRACAS, sep=';')
        region['pracas'] = region['pracas'].str.upper()
        listofregions = [tuple(x) for x in region.values]

        performance['region'][performance['region'].isnull()] = 'BR'
        for i in range(0, len(listofregions)):
            performance['region'][performance['campaign'].str.contains(listofregions[i][0], na=False)] = \
            listofregions[i][1]

        performance['region'] = performance['region'].apply(lambda x: unidecode.unidecode(x))

    performance['week'] = performance['date'].dt.week

    return performance


def performanceNewAppData():
    performance = pandas.concat([
        appsflyerData(),
        twitterdata(),
        facebookdata(['act_1894184000615284', 'act_1691552937545059', 'act_967083766658650']),
        googlereports(['771-742-8350', '411-922-6657']),
        baseDatabase()
    ])
    performance = cleanNewAppData(performance)
    performance = performance[
        ['date', 'week', 'tool', 'midia', 'source', 'os_name', 'campaign', 'adgroup', 'creative',
         'installs', 'first_trip', 'sign_ups', 'amount_spent', 'impressions', 'clicks', 'pax', 'trips', 'burn', 'gmv',
         'takerate', 'pft_pax',
         'pft_trips', 'pft_burn', 'pft_gmv', 'pft_takerate', 'region']]
    return performance


