from utils.paths import all_paths_tabelas, all_paths_relatorios
from init.init import googledriveinit
from pydrive.drive import GoogleDrive
import pandas
import os


def load_files():
    files_to_load = [f for f in os.listdir(all_paths_tabelas) if not f.startswith('.')]

    drive = GoogleDrive(googledriveinit())

    file_list = drive.ListFile({'q': "'folder_id' in parents and trashed=false"}).GetList()
    for file1 in file_list:
        print('[Google Drive] - File to load: %s, id: %s' % (file1['title'], file1['id']))
        if file1['title'] in files_to_load:
            file = drive.CreateFile({'id': file1['id']})
            file.GetContentFile(all_paths_tabelas + file1['title'])


def upload_files():
    files_to_upload = [f for f in os.listdir(all_paths_relatorios) if not f.startswith('.')]

    drive = GoogleDrive(googledriveinit())

    file_list = drive.ListFile({'q': "'folder_id' in parents and trashed=false"}).GetList()
    for file1 in file_list:
        print('[Google Drive] - File to upload: %s, id: %s' % (file1['title'], file1['id']))
        if file1['title'] in files_to_upload:
            file = drive.CreateFile({'id': file1['id']})
            file.SetContentFile(all_paths_relatorios + file1['title'])
            file['title'] = file1['title']
            file.Upload()
