from utils.paths import PATH_SIGLAS_PRACAS
from drivers.appsflyer import appsflyerData as appsflyerDriversData
from drivers.facebook import facebookdata as facebookdrivers
from drivers.google import googlereports as googledrivers
from drivers.database import BaseNewApp as baseNewApp
import pandas
import unidecode


def performanceDrivers():
    performancedrivers = pandas.concat([
        appsflyerDriversData(),
        facebookdrivers(['act_2013665502000466', 'act_1550209891679365']),
        googledrivers(['619-852-1756']),
        baseNewApp()])

    performancedrivers['week'] = performancedrivers['date'].dt.week
    performancedrivers = performancedrivers[['date', 'week', 'tool', 'midia', 'source', 'os_name', 'campaign', 'adgroup', 'creative', 'installs', 'criou_basic', 'enviou_todos_docs', 'amount_spent', 'impressions', 'clicks', 'signups', 'signups_with_migration', 'regulars', 'regulars_with_migration', 'dft', 'dft_with_migration', 'region']]

    performancedrivers['campaign'][performancedrivers['source'].str.contains('Driver_Acq', na=False)] = \
        performancedrivers['source']
    performancedrivers['source'][performancedrivers['source'].str.contains('Driver_Acq', na=False)] = \
        performancedrivers['source'].apply(
            lambda x: str(x)[str(x).find('_', 8) + 1:str(x).find('_', 11)] if str(x).count('_') > 2 else 'landing'
        )

    if 'region' in performancedrivers.columns:
        region = pandas.read_csv(PATH_SIGLAS_PRACAS, sep=';')
        region['pracas'] = region['pracas'].str.upper()
        listofregions = [tuple(x) for x in region.values]
        performancedrivers['region'][performancedrivers['region'].isnull()] = 'BR'
        for i in range(0, len(listofregions)):
            performancedrivers['region'][performancedrivers['campaign'].str.contains(listofregions[i][0], na=False)] = \
                listofregions[i][1]

            performancedrivers['region'] = performancedrivers['region'].apply(lambda x: unidecode.unidecode(x) if x is not None else 'BR')

        performancedrivers['week'] = performancedrivers['date'].dt.week
    return performancedrivers



