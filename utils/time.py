import time
import datetime

# This is only to get the current time. We're only looking for reports inside our current month.
# dateStart is always day 1 of the month we are currently in, the only if is if today is day 1, then we close the month.
# dateEnd is always day-1
# the xxxxdate() method returns date as yyyy-MM-dd (used in adjust, facebook and some querys of our database)
# the xxxxdatebase() method returns date as yyyyMMdd (used in google and some of our database querys)


def datestart(currentDate = None):
    #if today is the first day of the month we will get the first day of the last month.

    if currentDate:
        if currentDate.day == 1:
            today = '01'
            currmonth = currentDate.month
            month = int(currmonth) - 1
            year = currentDate.year
            if month < 10:
                return str(year) + '-' + '0' + str(month) + '-' + str(today)
            else:
                return str(year) + '-' + str(month) + '-' + str(today)
        else:
            today = '01'
            currdate = str(currentDate.year) + '-' + (str(currentDate.month) if currentDate.month > 9 else '0' + str(currentDate.month))
            return currdate + '-' + str(today)
    else:
        if time.strftime("%d") == '01':
            today = '01'
            currmonth = time.strftime('%m')
            month = int(currmonth) - 1
            year = time.strftime('%Y')
            if month < 10:
                return year + '-' + '0' + str(month) + '-' + today
            else:
                return year + '-' + str(month) + '-' + today
        else:
            today = '01'
            currdate = time.strftime("%Y-%m")
            return currdate + '-' + str(today)


def dateend():
    datetoend = str(datetime.datetime.now() - datetime.timedelta(days=1)).partition(" ")[0]
    return datetoend


def datetobase(date):
    return date.replace('-', '')


startdate = datestart()
startdatebase = datetobase(startdate)
enddate = dateend()
enddatebase = datetobase(enddate)
