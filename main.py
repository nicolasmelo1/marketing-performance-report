from drivers.performance import performanceDrivers as performancedrivers
from pax.performance import performanceNewAppData as performanceNewApp
from utils.paths import PATH_TO_RELATORIOS
from utils.drive import load_files, upload_files
import pandas
import logging
import warnings

warnings.filterwarnings("ignore")
pandas.options.mode.chained_assignment = None


load_files()
logging.info("Secondary Files Downloaded")
print('Secondary Files Downloaded')

performancedrivers().to_csv(PATH_TO_RELATORIOS + 'performancereportdrivers.csv', sep=',', float_format='%.0f', encoding='utf-8', index=False)
logging.info("[DRIVER] Performance Report Generated")
print('Driver - Performance Report Generated')

performanceNewApp().to_csv(PATH_TO_RELATORIOS + 'performancereportnewapp.csv', sep=',', float_format='%.0f', encoding='utf-8', index=False)
logging.info("[PAX] Performance Report Generated")
print('Pax - Performance Report Generated')

upload_files()
logging.info("Performance Reports Files Uploaded")
print('Performance Reports Files Uploaded')
