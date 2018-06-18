import pandas
import numpy
import datetime
import utils.time
from utils.apicalls import AppsFlyer
from utils.queries import QUERY_PAX_APPSFLYER_INSTALLS
from init.init import databseinit
from utils.paths import PATH_CUSTOS_APPSFLYER, PATH_DEFINE_VALUES, PATH_CUSTOS_GMAPS
from utils.apicalls import GoogleAds
import logging

def appsflyerData():

    # method for retrieving and make some ETL process of the Appsflyer Data

    # first we have appsflyer_data which hold appsflyer's data
    # then define values, which is a dataframe for some Classification Process.
    # custos and custosgmaps is to define the costs for some campaigns, as costs for google maps and costs
    # for other campaigs is calculated differently, we need two different files.
    appsflyer_data = pandas.DataFrame()
    define_values = pandas.read_csv(PATH_DEFINE_VALUES, sep=';')
    custosgmaps = pandas.read_csv(PATH_CUSTOS_GMAPS, sep=';')
    custos = pandas.read_excel(PATH_CUSTOS_APPSFLYER,
                               delimiter=';', keep_default_na=False,
                               na_values=['-1.#IND', '1.#QNAN', '1.#IND', '-1.#QNAN', '#N/A', 'N/A', '#NA', 'NULL',
                                          'NaN', '-NaN', 'nan', '-nan'])
    custos = custos.replace(r'', numpy.nan, regex=True)

    # format custos dataframe date, convert everything to datetime
    custos['End Date'] = pandas.to_datetime(pandas.Series(custos['End Date']), format="%d/%m/%Y")
    custos['Start Date'] = pandas.to_datetime(pandas.Series(custos['Start Date']), format="%d/%m/%Y")

    # config startdate and enddate and create a list of dates
    start = datetime.datetime.strptime(utils.time.startdate, "%Y-%m-%d")
    end = datetime.datetime.strptime(utils.time.enddate, "%Y-%m-%d")
    date_list = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1)]
    custos = custos[(custos['End Date'] > start) | (custos['End Date'].isnull())]

    # retrieve appsflyer Installations from our database
    appsflyer = retrieveAppsflyerInstalls()
    appsflyer.rename(
        columns={'install_time': 'date'}, inplace=True)

    # this is a dataframe passing daily_report, this is for retrieving clicks
    appsflyergmaps = AppsFlyer()
    appsflyergmaps = appsflyergmaps.reports(utils.time.startdate, utils.time.enddate,
                                            ['app_pax_android', 'app_pax_ios'],
                                            ['daily_report'])
    appsflyergmaps.rename(
        columns={
            'Date': 'date',
            'Media Source (pid)': 'source',
            'Campaign (c)': 'campaign',
            'Clicks': 'clicks'
        },
        inplace=True)
    appsflyergmaps['clicks'] = appsflyergmaps['clicks'].fillna(0)
    appsflyergmaps.drop([x for x in list(appsflyergmaps) if x not in ['date', 'source', 'campaign', 'clicks']], inplace=True, axis=1)

    # i just need the clicks from the source: "Google_Maps"
    appsflyer = pandas.concat([appsflyer, appsflyergmaps[appsflyergmaps['source']=='Google_Maps']])

    # appsflyerevents from orange and degrade app, the event is af_first_trip
    appsflyerfirst = AppsFlyer()
    appsflyerfirst = appsflyerfirst.reports(utils.time.startdate, utils.time.enddate,
                                            ['app_pax_android', 'app_pax_ios'],
                                            ['in_app_events_report', 'organic_in_app_events_report'],
                                            ['event_time', 'media_source', 'campaign', 'event_name'],
                                            ['app_pax_event'])
    appsflyerfirst['first_trip'] = 1
    appsflyerfirst.drop(['Event Name'], inplace=True, axis=1)
    appsflyerfirst['Campaign'] = appsflyerfirst['Campaign'].fillna('None')
    appsflyerfirst['Media Source'] = appsflyerfirst['Media Source'].fillna('NA')
    appsflyerfirst['Event Time'] = appsflyerfirst['Event Time'].apply(lambda x: str(x).split(' ')[0])
    appsflyerfirst = appsflyerfirst.groupby(['Event Time', 'Media Source', 'Campaign', 'os_name']).sum()
    appsflyerfirst = appsflyerfirst.reset_index()

    appsflyerfirst.rename(
        columns={
            'Event Time': 'date',
            'Media Source': 'source',
            'Campaign': 'campaign'
        }, inplace=True)


    # appsflyerevents from orange and degrade app, the event is af_sign_up
    appsflyersignup = AppsFlyer()
    appsflyersignup = appsflyersignup.reports(utils.time.startdate, utils.time.enddate,
                                              ['app_pax_android', 'app_pax_ios'],
                                              ['in_app_events_report', 'organic_in_app_events_report'],
                                              ['event_time', 'media_source', 'campaign', 'event_name'],
                                              ['app_pax_event_2'])
    appsflyersignup['sign_ups'] = 1

    appsflyersignup.drop(['Event Name'], inplace=True, axis=1)
    appsflyersignup['Campaign'] = appsflyersignup['Campaign'].fillna('None')
    appsflyersignup['Media Source'] = appsflyersignup['Media Source'].fillna('NA')
    appsflyersignup['Event Time'] = appsflyersignup['Event Time'].apply(lambda x: str(x).split(' ')[0])
    appsflyersignup = appsflyersignup.groupby(['Event Time', 'Media Source', 'Campaign', 'os_name']).sum()
    appsflyersignup = appsflyersignup.reset_index()

    appsflyersignup.rename(
        columns={
            'Event Time': 'date',
            'Media Source': 'source',
            'Campaign': 'campaign'
        }, inplace=True)

    appsflyerevents = pandas.concat([appsflyerfirst, appsflyersignup])
    appsflyer = pandas.concat([appsflyerevents, appsflyer])

    google = GoogleAds()
    google = google.reportcampaigns(customerId='771-742-8350')
    google.rename(
        columns={
                 'Campaign': 'campaign_name',
                 'Campaign ID': 'campaign'
                }, inplace=True)

    google = google.drop_duplicates(subset=['campaign'])
    appsflyer = pandas.merge(appsflyer, google, how='left', left_on='campaign', right_on='campaign', indicator=True)
    appsflyer['campaign'][appsflyer['_merge'] == 'both'] = appsflyer['campaign_name']
    appsflyer.drop(['campaign_name', '_merge'], inplace=True, axis=1)
    appsflyer['tool'] = 'AppsFlyer'

    appsflyer['midia'] = 'unpaid'
    define_values = define_values[define_values['app'] == 'pax']
    for i in ['source', 'midia']:
        define_aux = define_values[~define_values[i].isna()]
        for rows in [tuple(x) for x in define_aux.values]:
            if str(rows[2]) == 'nan':
                appsflyer[i][appsflyer['campaign'].str.contains(rows[3], na=False)] = rows[
                    1 if i == 'source' else 0]
            else:
                appsflyer[i][appsflyer['source'].str.contains(rows[2], na=False)] = rows[
                    1 if i == 'source' else 0]

    appsflyer = appsflyer[['date','tool','midia','source','os_name','campaign','adgroup','creative','installs', 'first_trip', 'sign_ups', 'clicks']]

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
    appsflyer_data = pandas.merge(appsflyer_data, custosgmaps, how='left', left_on='source', right_on='source')

    appsflyer_data['amount_spent'] = 0
    appsflyer_data['sign_ups'] = appsflyer_data['sign_ups'].fillna(0)
    appsflyer_data['clicks'] = appsflyer_data['clicks'].fillna(0)
    appsflyer_data['amount_spent'][appsflyer_data['typeg'] == 'cpg'] = round((appsflyer_data['sign_ups'].astype(float) *
                                                                       appsflyer_data['cost_phone_activated'].astype(
                                                                           float)) + (
                                                                              appsflyer_data['clicks'].astype(float) *
                                                                              appsflyer_data['cost_click'].astype(
                                                                                  float)))

    appsflyer_data['amount_spent'][appsflyer_data['type'] == 'cpi'] = appsflyer_data['installs'].astype(float) * appsflyer_data['payout'].astype(float)
    appsflyer_data['amount_spent'][appsflyer_data['type'] == 'cpa'] = appsflyer_data['first_trip'].astype(float) * appsflyer_data['payout'].astype(float)
    appsflyer_data.drop(['type', 'typeg'], inplace=True, axis=1)



    appsflyer_data['amount_spent'] = appsflyer_data['amount_spent'].astype(str)
    appsflyer_data['amount_spent'] = appsflyer_data['amount_spent'].apply(lambda x: str(x.replace('nan', '0.0')))
    appsflyer_data['amount_spent'] = appsflyer_data['amount_spent'].apply(lambda x: str(x.replace('.', ',')))
    appsflyer_data.drop(['payout', 'Start Date', 'End Date', 'cost_phone_activated', 'cost_click', 'clicks'], inplace=True, axis=1)

    logging.info("[PAX] Appsflyer Updated")
    print('Pax - Appsflyer Updated')
    return appsflyer_data.drop_duplicates().reset_index(drop=True)


def retrieveAppsflyerInstalls():
    conn = databseinit()
    curNewUsers = conn.cursor()
    curNewUsers.execute(QUERY_PAX_APPSFLYER_INSTALLS)
    col_names = []
    for x in curNewUsers.description:
        col_names.append(x[0])
    newusers = pandas.DataFrame(curNewUsers.fetchall(), columns=col_names)
    return newusers
