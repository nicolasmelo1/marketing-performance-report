import utils.time

import datetime

# Only Queries

'''
############################################
###              FUNCTIONS               ###
############################################
'''
datestart = datetime.datetime.strptime(utils.time.startdate, "%Y-%m-%d") - datetime.timedelta(days=30)
datestart = str(datestart).partition(" ")[0]

# EXAMPLE
'''
############################################
###               DRIVERS                ###
############################################
'''
#BASE OLD APP
QUERY_DRIVER_FIRST_TRIP = """SELECT *
    WHERE  date_value BETWEEN '""" + utils.time.startdate + """' AND '""" + utils.time.enddate + """'
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 1, 2"""
QUERY_DRIVER_NEW_REGULAR = """SELECT *
WHERE date_value BETWEEN '""" + utils.time.startdate + """' AND '""" + utils.time.enddate + """'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2"""
QUERY_DRIVER_SIGN_UP = """SELECT *
WHERE  date_value BETWEEN '""" + utils.time.startdate + """' AND '""" + utils.time.enddate + """'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2"""

#COHORT OLD APP
QUERY_DRIVER_COHORT = """
SELECT
 where to_char((install_date::DATE),'yyyy-MM-dd') BETWEEN '""" + datestart + """' AND '""" + utils.time.enddate + """'
   and activity_kind = 'install'
group by 1,2,3
order by 1,2,3"""

#BASE NEW APP
QUERY_DRIVER_DFT_GMV_NEWAPP = """
SELECT *
WHERE (to_date((ddb.reg_time), 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '11 hours') :: DATE BETWEEN '""" + utils.time.startdate + """' AND '""" + utils.time.enddate + """'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1
"""

QUERY_DRIVER_NEW_REGULAR_NEWAPP = """
SELECT *
WHERE (to_date((ddb.work_time), 'YYYY-MM-DD HH24:MI:SS') -
      INTERVAL '11 hours') :: DATE BETWEEN '""" + utils.time.startdate + """' AND '""" + utils.time.enddate + """'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1
"""

QUERY_DRIVER_APPSFLYER_INSTALLS = """SElECT *
WHERE app_id IN ('apps_drivers') and CONVERT_TIMEZONE('GMT', 'America/Sao_Paulo',install_time::timestamp)::DATE BETWEEN '""" + utils.time.startdate + """' AND '""" + utils.time.enddate + """'
GROUP BY 1,2,3,4,5
ORDER BY 1,2"""

'''
############################################
###                 PAX                  ###
############################################
'''

#NEW APP
QUERY_PAX_NEWAPP = """
SELECT *"""

QUERY_PAX_APPSFLYER_INSTALLS = """SELECT *
WHERE app_id IN ('apps_pax') and install_time::DATE BETWEEN '""" + utils.time.startdate + """' AND '""" + utils.time.enddate + """'
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2"""


TESTE_QUERY_PAX_MAU_REGIAO = """SELECT *
                        WHERE r.call_date BETWEEN '""" + utils.time.startdate + ' 00:00:00' + """"' AND '""" + utils.time.enddate + """' + ' 23:59:59'
                        GROUP BY 1, 2, 3, 4, 5
                        ORDER BY 1, 2"""
