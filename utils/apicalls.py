from facebook_business.adobjects import adsinsights
from facebook_business.adobjects import adaccount
from init.init import adwordsinit
from init.init import twitterinit
from init.init import facebookinit
from init.init import appsflyerinit
from pandas.io.json import json_normalize
import requests
import pandas
import datetime
import io
import time
import random
import facebookads.exceptions

#########################################
####                                 ####
####   This is for extracting data   ####
####                                 ####
#########################################


# class to make facebook api calls
class FacebookAds:
    def __init__(self):
        facebookinit()

    def reports(self, date_start, date_end, ad_accounts, extracted, reports=pandas.DataFrame()):
        ad_account = random.choice(ad_accounts)
        while ad_account in extracted:
            ad_account = random.choice(ad_accounts)

        print("[Facebook] - Extracting data for ad_account=%s" % (ad_account))
        account = adaccount.AdAccount(ad_account)

        insights = account.get_insights(fields=[
            adsinsights.AdsInsights.Field.date_start,
            adsinsights.AdsInsights.Field.campaign_name,
            adsinsights.AdsInsights.Field.adset_name,
            adsinsights.AdsInsights.Field.ad_name,
            adsinsights.AdsInsights.Field.impressions,
            adsinsights.AdsInsights.Field.clicks,
            adsinsights.AdsInsights.Field.spend,
        ], params={
            'level': adsinsights.AdsInsights.Level.ad,
            'time_increment': '1',
            'time_range': {
                'since': date_start,
                'until': date_end
            },
        }, async=True)

        time.sleep(20)

        results = []
        async_job = insights.remote_read()
        while async_job['async_status'] != 'Job Completed':
            print('[Facebook] - Percent completed from async run=' + str(async_job['async_percent_completion']))
            time.sleep(20)
            async_job = insights.remote_read()
        if async_job['async_status'] == 'Job Completed':
            print('[Facebook] - Percent completed from async run=' + str(async_job['async_percent_completion']))
            time.sleep(20)
            results = [x for x in insights.get_result()]

        if results:
            facebook = pandas.DataFrame(results,
                                        columns=['ad_name', 'adset_name', 'campaign_name', 'clicks', 'date_start',
                                                 'date_stop', 'impressions', 'spend'])
            facebook['spend'] = facebook['spend'].apply(lambda x: str(x.replace('.', ',')))
            facebook = facebook[
                ['date_start', 'date_stop', 'campaign_name', 'adset_name', 'ad_name', 'impressions', 'clicks', 'spend']]
            facebook.drop(['date_stop'], inplace=True, axis=1)
            facebook['date_start'] = pandas.to_datetime(pandas.Series(facebook['date_start']), format="%Y-%m-%d")
            facebook = facebook.sort_values(by='date_start')
            reports = pandas.concat([reports, facebook])

        else:
            facebook = pandas.DataFrame(results,
                                        columns=['ad_name', 'adset_name', 'campaign_name', 'clicks', 'date_start',
                                                 'date_stop', 'impressions', 'spend'])
            reports = pandas.concat([reports, facebook])

        extracted.append(ad_account)
        if sorted(extracted) != sorted(ad_accounts):
            return self.reports(date_start, date_end, ad_accounts, extracted, reports)
        else:
            return reports

# class to make google api calls
class GoogleAds:

    def reports(self, dateStart, dateEnd, customerId=None):
        print("[Google] - Extracting reports data from customer_id=%s" % (customerId))
        # initialize adwords
        adwords = adwordsinit(customerId)

        # watch the version, right now the version is 'v201710' it needs to be updated constantly.
        report_downloader = adwords.GetReportDownloader(version='v201802')

        # it's just a query, you can check more about the parameters here:
        # https://developers.google.com/adwords/api/docs/appendix/reports/campaign-performance-report?hl=pt-br#bidtype
        report_query = ('SELECT Date, CampaignName, Impressions, Interactions, Clicks, Cost '
                      'FROM CAMPAIGN_PERFORMANCE_REPORT '
                      'DURING ' + dateStart + ', ' + dateEnd)

        stream_data = report_downloader.DownloadReportAsStreamWithAwql(report_query,'CSV')

        # convert stream data to pandas, it doesn't give me a dataframe.
        data = pandas.read_csv(stream_data,
                           sep=str(','),
                           encoding='utf-8-sig',
                           header=1,
                           error_bad_lines=False,
                           warn_bad_lines=False)

        # Cost come as millions, the real data is the number divided by 1.000.000
        data['Cost'] = data['Cost'].apply(lambda x: round(x/1000000))

        # Delete last line {line from total values of google]
        data = data[:-1]

        # Convert to datetime and sort it
        data['Day'] = pandas.to_datetime(pandas.Series(data['Day']), format="%Y-%m-%d")
        data = data.sort_values(by='Day')
        return data


    #google api calls to extract campaign names with campaign ids
    def reportcampaigns(self, customerId=None):
        print("[Google] - Extracting campaigns from customer_id=%s" % (customerId))
        adwords = adwordsinit(customerId)
        report_downloader = adwords.GetReportDownloader(version='v201802')
        report_query = ('SELECT CampaignName, CampaignId '
                        'FROM CAMPAIGN_PERFORMANCE_REPORT')
        stream_data = report_downloader.DownloadReportAsStreamWithAwql(report_query, 'CSV')

        data = pandas.read_csv(stream_data,
                               sep=str(','),
                               encoding='utf-8-sig',
                               header=1,
                               error_bad_lines=False,
                               warn_bad_lines=False)
        data = data[:-1]
        return data


# class to make twitter api calls
class TwitterAds:

    # to get the campaigns of twitter using twitter api you'll want to get your hands "dirty"
    # different than google or fb api, twitter ads api is quite new and doesn't come with a lot of tools out of the box
    # you need to build it yourself
    def reportcampaigns(self, dateStart):
        # this gets all the campaigns and campaignids that we made with the account
        getcampaigns = 'https://ads-api.twitter.com/2/accounts/18ce54np2w4/campaigns'
        content = requests.get(getcampaigns, auth=twitterinit()).json()
        campaigns = json_normalize(content, ['data'])

        campaigns.drop(['updated_at', 'total_budget_amount_local_micro', 'start_time', 'standard_delivery', 'servable',
                        'funding_instrument_id', 'frequency_cap', 'entity_status', 'duration_in_days', 'deleted',
                        'daily_budget_amount_local_micro', 'currency', 'created_at', 'account_id'],
                       inplace=True,
                       axis=1)
        campaigns['end_time'] = campaigns['end_time'].apply(lambda x: str(x).split('T')[0] if x is not None else None)
        campaigns['end_time'] = pandas.to_datetime(pandas.Series(campaigns['end_time']), format="%Y-%m-%d")

        campaigns = campaigns[(campaigns['end_time'] >= dateStart) | (campaigns['reasons_not_servable'] != 'EXPIRED')]
        campaigns.drop(['end_time', 'reasons_not_servable'], inplace=True, axis=1)
        return campaigns


    def recursiveextractor(self, datestart, dateend, campaignslist, placement, twitterdataframe=pandas.DataFrame()):

        # this is the coolest part of the program. what happens is that, twitter doesn't give me by default
        # twitter data divided by date. so what i need to do is to do it myself.
        # i'll get then data from twitter from 24h date range. so i'll loop through this func until datestart is equal to dateend

        lastdate = datetime.datetime.strptime(datestart, "%Y-%m-%d") + datetime.timedelta(days=1)
        lastdate = str(lastdate).partition(" ")[0]
        # make the call
        content = requests.get('https://ads-api.twitter.com/2/stats/accounts/18ce54np2w4/', auth=twitterinit(),
                               params={
                                   'start_time': datestart+'T00:00:00-0300',
                                   'end_time': str(lastdate)+'T00:00:00-0300',
                                   'entity': 'CAMPAIGN',
                                   'granularity': 'TOTAL',
                                   'metric_groups': 'ENGAGEMENT,BILLING',
                                   'placement': placement,
                                   'entity_ids': campaignslist
                               }).json()

        # some json partition and normalization to convert it to dataframe
        campaignid = json_normalize(content, ['data'])
        campaignid.drop(['id_data'], inplace=True, axis=1)
        twitterreport = json_normalize(content['data'], 'id_data')
        twitterreport = pandas.concat(
            [twitterreport.drop('metrics', axis=1), pandas.DataFrame(twitterreport['metrics'].tolist())], axis=1)

        # drop what i don't need
        twitterreport.drop(['card_engagements', 'carousel_swipes', 'engagements', 'follows', 'likes', 'poll_card_vote',
                            'qualified_impressions', 'billed_engagements',
                            'replies', 'app_clicks', 'segment', 'tweets_send', 'url_clicks', 'retweets'], inplace=True, axis=1)

        # as date is something i've came up with, to get this data divided by date i need to put it myself on the DF
        twitterreport['date'] = datestart


        # some etl process in the metrics fields
        twitterreport['billed_charge_local_micro'] = twitterreport['billed_charge_local_micro'].apply(
            lambda x: 0 if x == None else int(round(sum(x)) / 1000000))
        twitterreport['impressions'] = twitterreport['impressions'].apply(lambda x: 0 if x == None else sum(x))
        twitterreport['clicks'] = twitterreport['clicks'].apply(lambda x: 0 if x == None else sum(x))

        twitterreport = pandas.concat([campaignid, twitterreport], axis=1)


        # twitter dataframe is what contains all the dataframes from each day
        twitterdataframe = twitterdataframe.append(twitterreport, ignore_index=True)

        if dateend == datestart:
            return twitterdataframe
        else:
            return self.recursiveextractor(str(lastdate), dateend, campaignslist, placement, twitterdataframe)


    def reports(self, dateStart, dateEnd):
        print("[Twitter] - Extracting twitter data")
        # this is what you call when you create a twitter object to retrieve the data
        # you need to pass only the Start Date and End Date Parameter
        # first you create an empty dataframe, that`ll hold the data inside the for loop
        twitter = pandas.DataFrame()

        # what this does is returning a dataframe with the following columns: campaign id and campaign name
        # in order to extract the performance data, you have to pass the campaign id you are trying to get the values
        # so what i do is extract all of our campaigns in this account
        twitterCampaigns = self.reportcampaigns(dateStart)

        # exceded number of campaigns is for saying that the number of campaigns i'm trying to retrieve the data is bigger than 20.
        # if we have more than 20 campaigns running we need to split it.
        excededNumberOfCampaigns = None

        # this converts the campaigns ids to a list
        twitterCampaignsList = twitterCampaigns['id'].tolist()


        # the placement is for saying that: you want campaigns from where?
        # "we have campaigns that can be distributed in our platform or outside, with third party publishers"
        # as we want both we create a list containing both.
        # This is because we can't make the twitter call passing 'ALL_ON_TWITTER' and 'PUBLISHER_NETWORK' in the same time.
        # we will need to make two calls
        placements = ['ALL_ON_TWITTER', 'PUBLISHER_NETWORK']


        # where the magic happens, first we need to loop through placements
        for placement in placements:
            twitterReportsDataFrame = pandas.DataFrame()

            # as i said, if it's bigger than 20 you need to split it, and there comes the campaignsList variable
            if len(twitterCampaignsList) > 20:
                excededNumberOfCampaigns = True
                campaignsList = twitterCampaignsList[:20]
            else:
                campaignsList = twitterCampaignsList

            # list to string, so i can make the call
            campaigns = ','.join(campaignsList)

            # as i said if the excededNumberOfCampaigns is true we need to split it.
            # the idea is simple: while twitterCampaignsList exists and it's not an empty list
            # you'll want to get only the values that aren't in campaignsList, that's why you override the variable with new values
            # then if twitterCampaignsList is still bigger than 20, you split it again consecutivelly.
            # you need the aux variable to don't override twitterCampaignsList, so on the next placement, you can start it off again
            # it's cool to see that, you get out of the while when the excededNumberOfCampaigns is False, so when campaignsList is less
            # than 20 in length, the final call happens outside the if clause, on the next step
            if excededNumberOfCampaigns is True:
                auxTwitterCampaignsList = twitterCampaignsList
                while excededNumberOfCampaigns is True:
                    twitterReportsDataFrame = pandas.concat([twitterReportsDataFrame, self.recursiveextractor(dateStart,
                                                                                                              dateEnd,
                                                                                                              campaigns,
                                                                                                              placement)])
                    auxTwitterCampaignsList = [x for x in auxTwitterCampaignsList if x not in campaignsList]
                    if len(auxTwitterCampaignsList) > 20:
                        excededNumberOfCampaigns = True
                        campaignsList = auxTwitterCampaignsList[:20]
                    else:
                        excededNumberOfCampaigns = False
                        campaignsList = auxTwitterCampaignsList
                    campaigns = ','.join(campaignsList)

            # in case it`s less than 20 campaigns you doesn't enter the if clause, so you just want to get the data
            twitterReportsDataFrame = pandas.concat([twitterReportsDataFrame, self.recursiveextractor(dateStart,
                                                                                                              dateEnd, campaigns, placement)])

            # remember our dataframe containing ids and campaign names? you use it to get the name of the campaigns.
            twitterreport = pandas.merge(twitterReportsDataFrame,
                               twitterCampaigns, how='inner', left_on='id', right_on='id')


            twitter = pandas.concat([twitter, twitterreport])
        twitter['date'] = pandas.to_datetime(pandas.Series(twitter['date']), format="%Y-%m-%d")
        twitter = twitter.sort_values(by='date')
        return twitter


#class to make adjust api calls
class Adjust:

    #this is to group the data, but as i stated the data already comes grouped
    '''
    groupingCategorizer = [
        ('date', ['day', 'hour', 'week', 'month']),
        ('tracker_name', ['trackers']),
        ('network', ['networks']),
        ('campaign', ['campaigns']),
        ('adgroup', ['adgroups']),
        ('creative', ['creatives']),
        ('country', ['countries']),
        ('device_type', ['device_types']),
        ('region', ['region']),
        ('os_name', ['os_names'])
    ]
    '''
    def reports(self, dateStart, dateEnd, appid, grouping, eventKpis, kpis=None, trackerFilter=None):
        eventKpisList = []


        for data in eventKpis:
            eventKpisList.append(data)


        kpis = ['installs'] if kpis is None else kpis
        trackerFilter = [''] if trackerFilter is None else trackerFilter

        trackerFilter = ','.join(trackerFilter)
        kpis = ','.join(kpis)
        eventKpisList = ','.join(eventKpisList)
        groupingList = ','.join(grouping)
        content = requests.get('https://api.adjust.com/kpis/v1/' + appid + '.csv',
                                            params={
                                                'start_date': dateStart,
                                                'end_date': dateEnd,
                                                'kpis': kpis,
                                                'event_kpis': eventKpisList,
                                                'user_token': 'user_token',
                                                'grouping': groupingList,
                                                'tracker_filter': trackerFilter
                                            }).content
        data = pandas.read_csv(io.StringIO(content.decode('utf-8')))
        data = data.rename(columns=eventKpis)
        data.drop(['tracker_token'], inplace=True, axis=1)
        data['date'] = pandas.to_datetime(pandas.Series(data['date']), format="%Y-%m-%d")
        data = data.sort_values(by='date')

        return data


#class to make appsflyer api calls
class AppsFlyer:
    def reports(self, dateStart, dateEnd, appNames, callTypes, fields=None, eventName=None, countApp=0, countCall=0, data=pandas.DataFrame(), saveEndDate=None):

        print("[Appsflyer] - Extracting appsflyer data for app_name=%s and call_type=%s" %
              (appNames[countApp], callTypes[countCall]))

        # this initializes every variable passed
        eventNameList = ','.join(eventName) if eventName is not None else ''
        fieldsList = ','.join(fields) if fields is not None else ''
        field = 'fields' if fields else ''
        events = 'event_name' if 'in_app' in callTypes[countCall] else ''
        dataFromCall = pandas.DataFrame()
        #the call get made to contentappsflyer
        try:
            contentappsflyer = requests.get('https://hq.appsflyer.com/export/' + appNames[countApp] + '/' + callTypes[countCall] + '/v5',
                                            params={
                                                'api_token': appsflyerinit(),
                                                'timezone': '-03:00',
                                                'from': dateStart,
                                                'to': dateEnd,
                                                field: fieldsList,
                                                events: eventNameList
                                            }).content
            # tranform the data recieved to dataframe
            dataFromCall = pandas.read_csv(io.StringIO(contentappsflyer.decode('utf-8')))
            if callTypes[countCall] in ['installs_report', 'in_app_events_report', 'uninstall_events_report', 'organic_installs_report', 'organic_in_app_events_report']:
                dataFromCall['Media Source'] = dataFromCall['Media Source'].fillna('no_channel')
                # some classification processes
                # the first is for which operational system is it for
                # the second is to set values if organic raw data gets called
                if 'organic' in callTypes[countCall]:
                    dataFromCall['Media Source'] = 'Organic'
                    dataFromCall['Campaign'] = '0'
                    dataFromCall['Adset'] = '0'
            if 'com.' in appNames[countApp]:
                dataFromCall['os_name'] = 'android'
            else:
                dataFromCall['os_name'] = 'ios'
        except:
            self.reports(dateStart=dateStart, dateEnd=dateEnd, appNames=appNames, callTypes=callTypes, fields=fields,
                         eventName=eventName, countApp=countApp,
                         countCall=countCall,
                         data=data, saveEndDate=saveEndDate)
        # The func has two counters in it so it can iterate over
        # This means the data retrieved is valid, so you go for the loop, there are 2 loops in this func, first run one then the other
        if len(dataFromCall.index) < 200000:
            #the app name was completed
            if len(appNames) == countApp+1:
                #the loop was completed
                if len(callTypes) == countCall+1:

                    data = pandas.concat([data, dataFromCall])
                    #now we return the data and get out of the func
                    return data
                else:
                    # If saveStartDate exists it replaces dateEnd parameter and sets itself to None so it can run nicely
                    if saveEndDate:
                        dateEnd= saveEndDate
                        saveEndDate = None
                    else:
                        pass
                    # The call fields are for the second loop, so for every new iteration the counter for appNames gets restarted and for calls gets added 1
                    data = pandas.concat([data, dataFromCall])
                    return self.reports(dateStart, dateEnd, appNames, callTypes, fields, eventName, countApp=0,
                             countCall=countCall+1,
                             data=data, saveEndDate=saveEndDate)
            else:
                # If saveStartDate exists it replaces dateEnd parameter and sets itself to None so it can run nicely [2
                if saveEndDate:
                    dateEnd = saveEndDate
                    saveEndDate = None
                else:
                    pass
                data = pandas.concat([data, dataFromCall])
                # The app fields are for the first loop, so for every new iteration the counter for calls stays the same and for app it adds 1
                return self.reports(dateStart, dateEnd, appNames, callTypes, fields=fields, eventName=eventName, countApp=countApp+1, countCall=countCall, data=data, saveEndDate=saveEndDate)
        else:
            # Where the magic happens, and why this is a recursive func
            # If the dataFrame have 2000000 rows it runs again without changing the counters
            # Also, the dateEnd parameter gets updated with the time of the last row of the dataFrame
            saveEndDate = dateEnd if saveEndDate is None else saveEndDate
            dateEnd = dataFromCall['Event Time' if 'event_time' in fields else 'Install Time'].iloc[-1][:-3]

            # The time comes as yyyy-mm-dd hh:mm:ss so i take out of the string ':ss' to update dateEnd parameter
            data = pandas.concat([data, dataFromCall])
            return self.reports(dateStart=dateStart, dateEnd=dateEnd, appNames=appNames, callTypes=callTypes, fields=fields, eventName=eventName, countApp=countApp,
                         countCall=countCall,
                         data=data, saveEndDate=saveEndDate)

