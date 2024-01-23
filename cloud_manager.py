import io

from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload

from console_print import c_print, MyColors as Color


class Cloud:
    def __init__(self, folder_id: str, service_account: dict) -> None:
        """
        Build a connection to a drive folder to save copy of the dataset metadata
        :param folder_id: the id of the Google Drive folder where to save the file
        :param service_account: the service_account info to build the connection, to pass as a dictionary
        """
        c_print.myprint("Building drive services", Color.YELL)
        self.folder_id = folder_id
        self.service_account = service_account
        self.temp_path = "file/temp/"

        try:
            credentials = Credentials.from_service_account_info(self.service_account)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            c_print.myprint("Drive services built correctly", Color.GREEN)

        except HttpError as e:
            raise CloudError(e)
        except Exception as e:
            raise CloudError(e)

    def search_folder_and_get_latest_file(self, dataset_id: str, cat: str) -> str:
        """
        When a dataset is uploaded for the first time, a folder is created to hold all of its metadata copy.
        This function search for the existence of a folder for the dataset, if the folder exist search in the folder for
        the latest copy of the metadata
        :param dataset_id: the id of the dataset to search for
        :param cat: the catalogue in which to look for the dataset. FOR ckanFAIR CKAN is the only catalogue
        :return: The Google Drive ID of the latest copy of the metadata of the dataset if they exist
        """
        try:
            query = (f"'{self.folder_id}' in parents and mimeType='application/vnd.google-apps.folder' "
                     f"and name='{dataset_id}'")
            response = self.drive_service.files().list(q=query).execute()

            if 'files' in response and len(response['files']) > 0:
                sub_folder = response['files'][0]
                c_print.myprint(f"Found sub-folder for dataset '{dataset_id}' with ID: {sub_folder['id']}", Color.BLUE)

                query = f"'{sub_folder['id']}' in parents and name contains '{cat + '-'}'"
                response = self.drive_service.files().list(q=query, orderBy='createdTime desc', pageSize=1).execute()

                if 'files' in response and len(response['files']) > 0:
                    file = response['files'][0]
                    c_print.myprint(f"Found file '{file['name']}' with ID: {file['id']}", Color.GREEN)
                    return file['id']
                else:
                    c_print.myprint(f"No file found for dataset '{dataset_id}' for '{cat + '-'}' catalogue", Color.YELL)
                    return ''
            else:
                c_print.myprint(f"No sub-folder found for dataset '{dataset_id}' in folder", Color.YELL)
                return ''
        except HttpError as e:
            raise CloudError(e)
        except Exception as e:
            raise CloudError(e)

    def search_folder_and_get_latest_file_for_resources(self, dataset_id: str, resource_id: str, cat: str) -> str:
        """
        When a dataset is uploaded for the first time, a folder is created to hold all of its data copy.
        This function search for the existence of a folder for the dataset, if the folder exist search in the folder for
        the latest copy of the data
        :param dataset_id: id of the dataset to search the resource for
        :param resource_id: id of the resource to search for
        :param cat: the catalogue in which to look for the dataset. FOR euFAIR4CKAN CKAN is the only catalogue
        :return:  The Google Drive ID of the latest copy of the data of the dataset if they exist
        """
        try:
            query = (f"'{dataset_id}' in parents and mimeType='application/vnd.google-apps.folder' "
                     f"and name='{resource_id}'")
            response = self.drive_service.files().list(q=query).execute()

            if 'files' in response and len(response['files']) > 0:
                sub_folder = response['files'][0]
                c_print.myprint(f"Found sub-folder for dataset '{resource_id}' with ID: {sub_folder['id']}", Color.BLUE)

                query = f"'{sub_folder['id']}' in parents and name contains '{cat + '-'}'"
                response = self.drive_service.files().list(q=query, orderBy='createdTime desc', pageSize=1).execute()

                if 'files' in response and len(response['files']) > 0:
                    file = response['files'][0]
                    c_print.myprint(f"Found file '{file['name']}' with ID: {file['id']}", Color.GREEN)
                    return file['id']
                else:
                    c_print.myprint(f"No file found for dataset '{resource_id}' for '{cat + '-'}' catalogue", Color.YELL)
                    return ''
            else:
                c_print.myprint(f"No sub-folder found for dataset '{resource_id}' in folder", Color.YELL)
                return ''
        except HttpError as e:
            raise CloudError(e)
        except Exception as e:
            raise CloudError(e)

    def download_file(self, file_id: str) -> str:
        """
        Download a file from the Google Drive folder
        :param file_id: the id of the file to download
        :return: the id of the file downloaded which overlap with the file name when downloaded
        """
        try:
            c_print.myprint(f"Download file -> {file_id}", Color.YELL)
            fh = io.FileIO(self.temp_path + file_id + '.txt', 'wb')
            downloader = self.drive_service.files().get_media(fileId=file_id)
            downloader_fh = downloader.execute()
            fh.write(downloader_fh)
            fh.close()
            c_print.myprint("File downloaded", Color.GREEN)

            return file_id
        except HttpError as e:
            raise CloudError(e)
        except FileNotFoundError as e:
            raise CloudError(e)
        except IOError as e:
            raise CloudError(e)
        except Exception as e:
            raise e

    def upload(self, file_name: str, dataset_id: str, f_type: str = 'txt') -> str:
        """
        Upload a file to the Google Drive folder
        :param file_name: the name of the file to upload
        :param dataset_id: the id of the database which the file correspond to
        :param f_type: the format of the file to upload
        :return: the id of the file once uploaded
        """
        try:
            file_path = self.temp_path + file_name + '.' + f_type
            query = (f"'{self.folder_id}' in parents and mimeType='application/vnd.google-apps.folder' "
                     f"and name='{dataset_id}'")
            response = self.drive_service.files().list(q=query).execute()

            if 'files' in response and len(response['files']) > 0:
                sub_folder = response['files'][0]
                c_print.myprint(f"Sub-folder for '{dataset_id}' ealready exist with ID: {sub_folder['id']}", Color.GREEN)
            else:
                sub_folder_metadata = {
                    'name': dataset_id,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [self.folder_id]
                }
                sub_folder = self.drive_service.files().create(body=sub_folder_metadata, fields='id').execute()
                c_print.myprint(f"Sub-folder '{dataset_id}' created with ID: {sub_folder['id']}", Color.GREEN)

            media = MediaFileUpload(file_path)
            file_metadata = {
                'name': file_name,
                'parents': [sub_folder['id']]
            }
            uploaded_file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            c_print.myprint(f"File '{file_name}' uploaded with ID: {uploaded_file['id']}", Color.GREEN)

            return uploaded_file['id']
        except HttpError as e:
            raise CloudError(e)
        except Exception as e:
            raise CloudError(e)


class CloudError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error happening to write, open or create a file/folder on the drive
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")