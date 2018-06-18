import os

# This is step is for getting the path to the folders, you have 2 aux folders
# one is called Tabelas which are auxiliary tables, the other is Relatorios
# where you'll put all the generated csvs


# This is the main path, the others are static and doesn't change

path = '/'.join(os.path.realpath(__file__).replace('\\', '/').split('/')[:-2])

'''
############################################
###    !!!!!    DON'T CHANGE     !!!!!   ###
############################################
'''

#This is the paths to write and read reports or tables
all_paths_tabelas = path + r'/staticfiles/tables/'
all_paths_relatorios = path + r'/staticfiles/reports/'

'''
############################################
###                 READ                 ###
############################################
'''

#PAX OLD APP
PATH_CUSTOS_ADJUST = all_paths_tabelas + 'custos_adjust.csv'
PATH_CUSTOS_GMAPS = all_paths_tabelas + 'custos_gmaps.csv'

#DRIVERS
PATH_CUSTOS_ADJUST_DRIVERS = all_paths_tabelas + 'custos_adjust_drivers.csv'
PATH_TO_DRIVER_CHANNEL_EQUIVALENCE = all_paths_tabelas + 'source_and_campaigns_by_driverchanelid.csv'

#PAX AND DRIVERS
PATH_CUSTOS_APPSFLYER = all_paths_tabelas + 'custos_appsflyer_newapp.xlsx'
PATH_DEFINE_VALUES = all_paths_tabelas + 'media_and_source_by_source_and_campaign_names.csv'
#PAX
PATH_SIGLAS_PRACAS = all_paths_tabelas + 'siglas_pracas.csv'


'''
############################################
###                WRITE                 ###
############################################
'''

#AUTOMATICALLY
PATH_TO_CUSTOS_ADJUST = all_paths_tabelas + 'custos_adjust.csv'
PATH_TO_CUSTOS_ADJUST_DRIVERS = all_paths_tabelas + 'custos_adjust_drivers.csv'

#MANUAL
PATH_TO_RELATORIOS = all_paths_relatorios