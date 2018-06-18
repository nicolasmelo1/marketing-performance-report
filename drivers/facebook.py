from utils.apicalls import FacebookAds
import utils.time
import logging


def facebookdata(ad_accounts):
    facebook = FacebookAds()
    facebook = facebook.reports(date_start=utils.time.startdate, date_end=utils.time.enddate, ad_accounts=ad_accounts, extracted=list())
    facebook['midia'] = 'paid'
    facebook['source'] = 'facebook'
    facebook['os_name'] = 'android'
    facebook['network'] = 'Facebook Ads'

    facebook['source'][facebook['adset_name'].str.contains('_INS')] = 'instagram'
    facebook['os_name'][facebook['campaign_name'].str.contains('IOS')] = 'ios'
    facebook['network'][facebook['adset_name'].str.contains('_INS')] = 'Instagram Installs'

    facebook.rename(columns={'date_start': 'date', 'campaign_name': 'campaign', 'adset_name': 'adgroup', 'ad_name': 'creative', 'spend':'amount_spent'}, inplace=True)
    facebook['tool'] = 'FaceAds'
    facebook = facebook[
        ['date','tool','midia', 'source', 'os_name', 'network',  'campaign', 'adgroup', 'creative', 'amount_spent', 'impressions', 'clicks']]

    logging.info("[DRIVER] Facebook Updated")
    print('Driver - Facebook Updated')
    return facebook
