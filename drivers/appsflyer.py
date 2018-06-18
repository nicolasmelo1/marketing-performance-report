import numpy
import logging
import pandas
import requests
import datetime
import utils.time
from utils.paths import PATH_CUSTOS_APPSFLYER, PATH_DEFINE_VALUES
from utils.queries import QUERY_DRIVER_APPSFLYER_INSTALLS
from utils.apicalls import GoogleAds, AppsFlyer
from init.init import databseinit


def appsflyerData():
    appsflyer_data = pandas.DataFrame()
    define_values = pandas.read_csv(PATH_DEFINE_VALUES, sep=';')
    custos = pandas.read_excel(PATH_CUSTOS_APPSFLYER,
                               delimiter=';', keep_default_na=False,
                               na_values=['-1.#IND', '1.#QNAN', '1.#IND', '-1.#QNAN', '#N/A', 'N/A', '#NA', 'NULL',
                                          'NaN', '-NaN', 'nan', '-nan'])
    custos = custos.replace(r'', numpy.nan, regex=True)

    # format dataframes
    custos['End Date'] = pandas.to_datetime(pandas.Series(custos['End Date']), format="%d/%m/%Y")
    custos['Start Date'] = pandas.to_datetime(pandas.Series(custos['Start Date']), format="%d/%m/%Y")

    # config startdate and enddate and create a list of dates
    start = datetime.datetime.strptime(utils.time.startdate, "%Y-%m-%d")
    end = datetime.datetime.strptime(utils.time.enddate, "%Y-%m-%d")
    date_list = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1)]
    custos = custos[(custos['End Date'] > start) | (custos['End Date'].isnull())]

    appsflyer = AppsFlyer()
    appsflyer = appsflyer.reports(utils.time.startdate, utils.time.enddate,
                                  ['app_driver_android', 'app_driver_ios'],
                                  ['in_app_events_report', 'organic_in_app_events_report'],
                                  ['event_time', 'media_source', 'campaign', 'af_adset', 'event_name'],
                                  ['app_driver_event'])
    appsflyer['Campaign'] = appsflyer['Campaign'].fillna('None')
    appsflyer['Adset'] = appsflyer['Adset'].fillna('None')
    appsflyer['Media Source'] = appsflyer['Media Source'].fillna('NA')
    appsflyer['enviou_todos_docs'] = 1

    appsflyer.drop(['Event Name'], inplace=True, axis=1)
    appsflyer['Event Time'] = appsflyer['Event Time'].apply(lambda x: x.split(' ')[0])
    appsflyer = appsflyer.groupby(['Event Time', 'Media Source', 'Campaign','Adset', 'os_name']).sum()
    appsflyer = appsflyer.reset_index()
    appsflyer = appsflyer.rename(columns={
        'Event Time': 'date',
        'Media Source': 'source',
        'Campaign': 'campaign',
        'Adset': 'adgroup'
    })

    appsflyer = pandas.concat([appsflyer, retrieveAppsflyerInstalls()])

    google = GoogleAds()
    google = google.reportcampaigns()
    google.rename(
        columns={
            'Campaign': 'campaign_name',
            'Campaign ID': 'campaign'
        }, inplace=True)

    google = google.drop_duplicates(subset=['campaign'])

    appsflyer = pandas.merge(appsflyer, google, how='left', left_on='campaign', right_on='campaign', indicator=True)
    appsflyer['campaign'][appsflyer['_merge'] == 'both'] = appsflyer['campaign_name']
    appsflyer.drop(['campaign_name', '_merge'], inplace=True, axis=1)


    appsflyer['campaign'] = appsflyer['campaign'].fillna('None')
    appsflyer['source'] = appsflyer['source'].fillna('NA')
    appsflyer['tool'] = 'AppsFlyer'
    appsflyer['midia'] = 'nonpaid'
    define_values = define_values[define_values['app'] == 'driver']
    for i in ['source', 'midia']:
        define_aux = define_values[~define_values[i].isna()]
        for rows in [tuple(x) for x in define_aux.values]:
            if str(rows[2]) == 'nan':
                appsflyer[i][appsflyer['campaign'].str.contains(rows[3], na=False)] = rows[
                    1 if i == 'source' else 0]
            else:
                appsflyer[i][appsflyer['source'].str.contains(rows[2], na=False)] = rows[
                    1 if i == 'source' else 0]

    appsflyer['campaign'][appsflyer['source'].str.contains('Driver_Acq', na=False)] = \
        appsflyer[
        'source']

    appsflyer = appsflyer[['date','midia', 'tool', 'source', 'os_name', 'campaign', 'adgroup', 'installs', 'enviou_todos_docs']]

    appsflyer['date'] = pandas.to_datetime(pandas.Series(appsflyer['date']), format="%Y-%m-%d")
    for date in date_list:
        # concat everything on the go
        appsflyer_data = pandas.concat([appsflyer_data,
                                   pandas.merge(
                                       (appsflyer[appsflyer['date'] == date]),
                                       (custos[((custos['End Date'] >= date) | (custos['End Date'].isnull())) & (
                                               custos['Start Date'] <= date)]),
                                       how='left',
                                       left_on='campaign',
                                       right_on='campaign')])

    appsflyer_data['amount_spent'] = 0
    appsflyer_data['amount_spent'][appsflyer_data['type'] == 'cpi'] = appsflyer_data['installs'].astype(float) * appsflyer_data['payout'].astype(float)
    appsflyer_data['amount_spent'][appsflyer_data['type'] == 'cpl'] = appsflyer_data['enviou_todos_docs'].astype(float) * appsflyer_data['payout'].astype(float)

    appsflyer_data['amount_spent'] = appsflyer_data['amount_spent'].astype(str)
    appsflyer_data['amount_spent'][appsflyer_data['amount_spent'] == 'nan'] = '0.0'
    appsflyer_data['amount_spent'] = appsflyer_data['amount_spent'].apply(lambda x: str(x.replace('.', ',')))

    appsflyer_data['installs'] = appsflyer_data['installs'].fillna(0)
    appsflyer_data['criou_basic'] = 0
    appsflyer_data['criou_basic'] = appsflyer_data['installs']*0.36
    appsflyer_data['criou_basic'] = appsflyer_data['criou_basic'].apply(lambda x: round(x))
    appsflyer_data['criou_basic'] = appsflyer_data['criou_basic'].astype(int)

    appsflyer_data.drop(['type', 'payout', 'Start Date', 'End Date'], inplace=True, axis=1)

    appsflyer_data = appsflyer_data.sort_values(by='date')

    logging.info("[DRIVER] Appsflyer Updated")
    print('Driver - Appsflyer Updated')

    return appsflyer_data.drop_duplicates().reset_index(drop=True)

def retrieveAppsflyerInstalls():
    conn = databseinit()
    curNewUsers = conn.cursor()
    curNewUsers.execute(QUERY_DRIVER_APPSFLYER_INSTALLS)
    col_names = []
    for x in curNewUsers.description:
        col_names.append(x[0])
    newusers = pandas.DataFrame(curNewUsers.fetchall(), columns=col_names)
    return newusers
