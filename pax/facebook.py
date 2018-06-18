from utils.apicalls import FacebookAds
import utils.time
import logging
# Data organization and return full Facebook Data


def facebookdata(ad_accounts):
    facebook = FacebookAds()
    facebook = facebook.reports(date_start=utils.time.startdate, date_end=utils.time.enddate, ad_accounts=ad_accounts, extracted=list())
    facebook['tool'] = 'FaceAds'
    facebook['source'] = 'Facebook Ads'
    facebook['os_name'] = 'android'
    facebook['midia'] = 'paid'
    facebook['os_name'][facebook['campaign_name'].str.contains('iOS')] = 'ios'
    facebook['os_name'][facebook['campaign_name'].str.contains('IOS')] = 'ios'

    facebook.rename(columns={'date_start': 'date', 'campaign_name': 'campaign', 'adset_name': 'adgroup',
                             'ad_name': 'creative', 'spend': 'amount_spent'}, inplace=True)
    facebook = facebook[
        ['date', 'tool', 'midia', 'source', 'os_name', 'campaign', 'adgroup', 'creative', 'amount_spent', 'impressions',
         'clicks']]

    logging.info("[PAX] Facebook Updated")
    print('Pax - Facebook Updated')
    return facebook
