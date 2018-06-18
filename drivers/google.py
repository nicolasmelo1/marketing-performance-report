from utils.apicalls import GoogleAds
import utils.time
from utils.paths import PATH_DEFINE_VALUES
import pandas
import logging


def googlereports(customer_ids):
    reports = pandas.DataFrame()
    for customer_id in customer_ids:
        data = GoogleAds()
        data = data.reports(dateStart=utils.time.startdatebase, dateEnd=utils.time.enddatebase, customerId=customer_id)

        data.drop(['Clicks'], inplace=True, axis=1)
        # Column creation
        data['midia'] = 'paid'
        data['source'] = 'Google'
        data['network'] = 'Google Search - New Blue'
        data['os_name'] = 'android'

        # Convert values based on data on created tables

        define_values = pandas.read_csv(PATH_DEFINE_VALUES, sep=';')
        define_values = define_values[(define_values['app'] == 'driver') & (~define_values['source'].isna()) &
                                      (~define_values['campaign contains'].isna())]

        for rows in [tuple(x) for x in define_values.values]:
            data['source'][data['Campaign'].str.contains(rows[3], na=False)] = rows[1]

        data['os_name'][data['Campaign'].str.contains('_iOS')] = 'ios'
        data.rename(
            columns={'Day': 'date',
                     'Campaign': 'campaign',
                     'Cost': 'amount_spent',
                     'Impressions': 'impressions',
                     'Interactions': 'clicks'
                     }, inplace=True)
        data['tool'] = 'Adwords'
        reports = pandas.concat([reports, data])

    reports = reports[['date','tool', 'midia', 'source', 'os_name',
                       'network', 'campaign', 'amount_spent', 'impressions', 'clicks']]

    logging.info("[DRIVER] Google Updated")
    print('Driver - Google Updated')
    return reports


