import pandas
from init.init import databseinit as databaseinit
from utils.time import startdate
from utils.time import enddate
from utils.queries import QUERY_PAX_NEWAPP
from utils.paths import PATH_DEFINE_VALUES
import numpy
import logging


def baseDatabase():
    #reportnewApppaxData = pandas.read_csv(r'/Users/nicolasmelo1/Desktop/ToDab Corrected.csv', sep=',')
    define_values = pandas.read_csv(PATH_DEFINE_VALUES, sep=';')
    reportnewApppaxData = extractDatabasePFTsNewApp()
    if reportnewApppaxData.empty:
        reports = reportnewApppaxData
        reports['date'] = None
        reports['tool'] = None
        reports['midia'] = None
        reports['source'] = None
        reports['pax'] = None
        reports['trips'] = None
        reports['burn'] = None
        reports['gmv'] = None
        reports['takerate'] = None
        reports['pft_pax'] = None
        reports['pft_trips'] = None
        reports['pft_burn'] = None
        reports['pft_gmv'] = None
        reports['pft_takerate'] = None
        reports['region'] = None
        return reports
    reportnewApppaxData.columns = ['call_date', 'channel', 'metropolitan_area_name', 'pax', 'trips', 'burn', 'gmv',
                                   'takerate', 'call_date.1', 'channel.1', 'metropolitan_area_name.1', 'pft_pax',
                                   'pft_trips', 'pft_burn', 'pft_gmv', 'pft_takerate']

    reportsPFT = pandas.DataFrame()
    reportsPFT = reportsPFT.append(reportnewApppaxData)
    reportnewApppaxData.drop(
        ['call_date.1', 'channel.1', 'metropolitan_area_name.1', 'pft_pax', 'pft_trips', 'pft_burn', 'pft_gmv',
         'pft_takerate'], inplace=True, axis=1)
    reportsPFT.drop(['call_date', 'channel', 'metropolitan_area_name', 'pax', 'trips', 'burn', 'gmv', 'takerate'],
                    inplace=True, axis=1)

    reportsPFT.rename(columns={'call_date.1': 'call_date', 'channel.1': 'channel',
                               'metropolitan_area_name.1': 'metropolitan_area_name'}, inplace=True)

    reportsPFT = reportsPFT.dropna(axis=0, how='all')

    reports = pandas.concat([reportnewApppaxData, reportsPFT])
    reports['channel'] = reports['channel'].replace(r'', numpy.nan, regex=True)
    reports = reports.replace(numpy.nan, 0, regex=True)
    reports['channel'][reports['channel'] == 0] = 'Glispa'
    reports['metropolitan_area_name'][reports['metropolitan_area_name'] == 0] = 'BR'
    reports['metropolitan_area_name'][reports['metropolitan_area_name'].str.contains('Campos')] = 'SAO JOSE DOS CAMPOS'
    reports['metropolitan_area_name'] = reports['metropolitan_area_name'].str.upper()

    reports.rename(columns={'channel': 'source'}, inplace=True)

    reports['source'][reports['source'].str.contains('google')] = 'Google'


    reports['tool'] = 'Base'
    reports['midia'] = 'unpaid'
    define_values = define_values[(define_values['app'] == 'pax') & (define_values['source'].isna()) & (
    define_values['campaign contains'].isna())]
    for rows in [tuple(x) for x in define_values.values]:
        reports['midia'][reports['source'].str.contains(rows[2], na=False)] = rows[0]

    #reports['midia'][reports['campaign'].str.contains('Perf')] = 'paid'
    #reports['midia'][reports['campaign'].str.contains('Brand')] = 'paid'
    reports['midia'][reports['source'].str.contains('Organic')] = 'organic'

    reports['call_date'] = pandas.to_datetime(pandas.Series(reports['call_date']), format="%Y-%m-%d")
    #reports['week'] = reports['call_date'].dt.week

    reports.rename(columns={'call_date': 'date',
                               'metropolitan_area_name': 'region'}, inplace=True)
    reports = reports[
        ['date', 'tool', 'midia', 'source', 'pax', 'trips', 'burn', 'gmv', 'takerate', 'pft_pax',
         'pft_trips', 'pft_burn', 'pft_gmv', 'pft_takerate', 'region']]
    reports = reports[(reports['date'] >= startdate) & (reports['date'] <= enddate)]
    logging.info("[PAX] Database Updated")
    print('Pax - Database Updated')
    return reports



def extractDatabasePFTsNewApp():
    conn = databaseinit()
    curNewUsers = conn.cursor()
    curNewUsers.execute(QUERY_PAX_NEWAPP)
    newusers = pandas.DataFrame(curNewUsers.fetchall())
    return newusers

