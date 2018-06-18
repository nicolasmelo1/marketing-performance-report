from utils.time import startdate
from utils.apicalls import TwitterAds
import utils.time
import logging


def twitterdata():
    twitter = TwitterAds()
    twitter = twitter.reports(dateStart=utils.time.startdate, dateEnd=utils.time.enddate)
    twitter.drop(['id'], inplace=True, axis=1)
    twitter['tool'] = 'TwitterAds'
    twitter['source'] = 'Twitter'
    twitter['os_name'] = 'android'
    twitter['midia'] = 'paid'
    twitter['os_name'][twitter['name'].str.contains('iOS')] = 'ios'
    twitter['os_name'][twitter['name'].str.contains('IOS')] = 'ios'

    twitter.rename(columns={'billed_charge_local_micro': 'amount_spent', 'name': 'campaign'}, inplace=True)

    twitter = twitter[
        ['date', 'tool', 'midia', 'source', 'os_name', 'campaign', 'amount_spent', 'impressions',
         'clicks']]

    logging.info("[PAX] Twitter Updated")
    print('Pax - Twitter Updated')
    return twitter
