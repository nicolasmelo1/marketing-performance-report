from init.init import databseinit as databaseinit
from utils.paths import PATH_TO_DRIVER_CHANNEL_EQUIVALENCE, PATH_DEFINE_VALUES
from utils.queries import QUERY_DRIVER_SIGN_UP_NEWAPP, QUERY_DRIVER_NEW_REGULAR_NEWAPP, QUERY_DRIVER_DFT_NEWAPP
from utils.apicalls import GoogleAds
import unidecode
import pandas
import logging


def BaseNewApp():
    define_values = pandas.read_csv(PATH_DEFINE_VALUES, sep=';')

    reports = pandas.DataFrame()
    for query in [QUERY_DRIVER_NEW_REGULAR_NEWAPP, QUERY_DRIVER_DFT_NEWAPP, QUERY_DRIVER_SIGN_UP_NEWAPP]:
        aux = runQuery(query)

        #primeiro leio o csv de consulta, para virar um dataframe
        driver_channels = pandas.read_csv(
            PATH_TO_DRIVER_CHANNEL_EQUIVALENCE, sep=';')

        #depois junto as tabelas
        aux = pandas.merge(aux, driver_channels, how='left', left_on='driver_channel', right_on='driver_channel',
                           indicator=True)

        #aqui eu atribuo primeiro ao campo "source" o campo "source_name" caso o "driver_channel" exista em ambas as tabelas.
        #faço o mesmo para o campo "campaign"
        aux['source'][aux['_merge'] == 'both'] = aux['source_name']
        aux['campaign'][aux['_merge'] == 'both'] = aux['campaign_name']
        aux.drop(['source_name', 'campaign_name', 'driver_channel', '_merge'], inplace=True, axis=1)


        reports = pandas.concat([reports, aux])

    google = GoogleAds()
    google = google.reportcampaigns()
    google.rename(
        columns={
            'Campaign': 'campaign_name',
            'Campaign ID': 'campaign'
        }, inplace=True)

    google = google.drop_duplicates(subset=['campaign'])

    reports = pandas.merge(reports, google, how='left', left_on='campaign', right_on='campaign', indicator=True)
    reports['campaign'][reports['_merge'] == 'both'] = reports['campaign_name']
    reports.drop(['campaign_name', '_merge'], inplace=True, axis=1)

    reports['region'] = reports['region'].str.upper()

    reports['tool'] = 'BaseNewApp'
    reports['midia'] = 'nonpaid'
    define_values = define_values[define_values['app'] == 'driver']
    for i in ['source', 'midia']:
        define_aux = define_values[~define_values[i].isna()]
        for rows in [tuple(x) for x in define_aux.values]:
            if str(rows[2]) == 'nan':
                reports[i][reports['campaign'].str.contains(rows[3], na=False)] = rows[
                    1 if i == 'source' else 0]
            else:
                reports[i][reports['source'].str.contains(rows[2], na=False)] = rows[
                    1 if i == 'source' else 0]

    reports['date'] = pandas.to_datetime(pandas.Series(reports['date']), format="%Y-%m-%d")
    reports['week'] = reports['date'].dt.week

    reports['region'] = reports['region'].apply(lambda x: unidecode.unidecode(x))

    reports = reports[['date', 'week', 'tool', 'midia', 'source', 'campaign', 'signups', 'signups_with_migration', 'regulars', 'regulars_with_migration', 'dft', 'dft_with_migration', 'region']]

    logging.info("[DRIVER] Database Updated")
    print('Driver - Database Updated')
    return reports



def runQuery(query):
    conn = databaseinit()
    cursor = conn.cursor()
    cursor.execute(query)
    col_names = []
    for x in cursor.description:
        col_names.append(x[0])
    data = pandas.DataFrame(cursor.fetchall(), columns=col_names)
    return data


def no_channelDistribution(data, distribute_by, columns_to_group, filter_by = None, drop_in_distribution = None, secondDataframe = None):
    data = data.fillna('None')
    if filter_by:
        column = 0
        value = 0
        for columns, values in filter_by.items():
            column = columns
            value = values
        onlyNA = data[(data[column] == value)]
        temponlyValidChannels = data[(data[column] != value)]
        if drop_in_distribution:
            drop_in_na = drop_in_distribution + [column]
            temponlyValidChannels.drop(drop_in_distribution, inplace=True, axis=1)
            onlyNA.drop(drop_in_na, inplace=True, axis=1)
        else:
            drop_in_na = ['campaign']
    else:

        onlyNA = secondDataframe
        temponlyValidChannels = data
        drop_in_na = ['campaign']

    onlyNA = onlyNA.groupby([x for x in columns_to_group if x not in drop_in_na]).sum()
    onlyNA = onlyNA.reset_index()
    temponlyValidChannels = temponlyValidChannels.groupby(columns_to_group).agg(
        {[x for x in list(temponlyValidChannels) if x not in columns_to_group][0]: 'sum'})
    temponlyValidChannels = temponlyValidChannels.groupby([x for x in columns_to_group if x not in distribute_by]).apply(
        lambda x: x / float(x.sum()))
    temponlyValidChannels = temponlyValidChannels.reset_index()

    temponlyValidChannels.rename(columns={[x for x in list(temponlyValidChannels) if x not in columns_to_group][0]: [x for x in list(temponlyValidChannels) if x not in columns_to_group][0]+'_%'}, inplace=True)
    temponlyValidChannels = pandas.merge(onlyNA, temponlyValidChannels, how='left',
                                         left_on=[item for item in columns_to_group if item not in distribute_by],
                                         right_on=[item for item in columns_to_group if item not in distribute_by])

    temponlyValidChannels[[x for x in list(temponlyValidChannels) if x not in columns_to_group][0]] = round(temponlyValidChannels[[x for x in list(temponlyValidChannels) if x not in columns_to_group][0]] *
                                                                                                            temponlyValidChannels[[x for x in list(temponlyValidChannels) if x not in columns_to_group][0]+'_%'])
    temponlyValidChannels.drop([[x for x in list(temponlyValidChannels) if x not in columns_to_group][0]+'_%'], inplace=True, axis=1)

    return temponlyValidChannels
