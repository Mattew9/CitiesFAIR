import requests
from typing import Union

from file_manager import file_manager
from console_print import c_print, MyColors as Color


def get_dataset_distribution(url: str) -> int:
    try:
        is_file = 0
        acc_formats = file_manager.open_file('format', 'json', True, 'set').keys()
        for file_format in acc_formats:
            if url.endswith('.' + file_format):
                is_file = 1
        c_print.myprint(f"TRYING TO RETRIEVE  TO {url}", Color.YELL)
        response = requests.get(f"{url}")
        if is_file:
            c_print.myprint("FILE COMPATIBLE", Color.GREEN)
            return response.status_code
        else:
            c_print.myprint(f"FILE NOT RETRIEVABLE OR NOT SUPPORTED", Color.RED)
            return 0
    except requests.RequestException as e:
        raise DataRetrieverError(e)
    except Exception as e:
        raise DataRetrieverError(e)


def retrieve_dataset(url: str) -> str:
    try:
        dataset = requests.get(url)
        if dataset.status_code not in range(200, 400):
            c_print.myprint("ERROR: connection error", Color.RED)
            return ''
        dataset_name = url.split("/")[-1]
        c_print.myprint("LOG: connection established. downloading file", Color.GREEN)
        file_manager.write_file(dataset_name, str(dataset.content), "temp/")
        return dataset_name

    except requests.RequestException as e:
        raise DataRetrieverError(e)
    except Exception as e:
        raise DataRetrieverError(e)


class DataRetriever:
    def __init__(self, url: str):
        """
        Create a data retriever object that allows to operate on a portal to retrieve data and metadata through CKAN API
        :param url: url of the CKAN compatible portal
        """
        if url[-1] == '/':
            self.url = url[0:-1]
        else:
            self.url = url

    def get_url(self) -> str:
        """
        :return: The url of the portal
        """
        return self.url

    def try_ckan(self) -> Union[str, None]:
        """
        Try to make a CKAN call using the given URL to test CKAN compatibility
        :return: True if CKAN is supported, None if test fail
        """
        try:
            c_print.myprint(f"TRYING TO MAKE A CKAN CALL TO {self.url}", Color.YELL)
            response = requests.get(f"{self.url}/api/3/action/package_list")
            if response.status_code == 200:
                c_print.myprint("CKAN COMPATIBLE", Color.GREEN)
                if response and response.json()['success']:
                    c_print.myprint("DATASET LIST CORRECTLY RETRIEVED", Color.GREEN)
                    return self.url
                else:
                    c_print.myprint("CAN NOT RETRIEVE THE DATASET LIST", Color.RED)
            else:
                c_print.myprint(f"CKAN API NOT SUPPORTED FOR {self.url}", Color.RED)
                raise DataRetrieverError
            return None
        except requests.RequestException as e:
            raise DataRetrieverError(e)
        except Exception as e:
            raise DataRetrieverError(e)

    def get_package_list(self) -> list:
        """
        Retrieve the dataset list of the portal
        :return: an array with the datasets IDs if found, empty list otherwise
        """
        try:
            c_print.myprint(f"TRYING TO RETRIEVE DATASET LIST", Color.YELL)
            response = requests.get(f"{self.url}/api/3/action/package_list")
            if response.status_code == 200:
                payload = response.json()
                if payload and payload['success']:
                    c_print.myprint("DATASET LIST CORRECTLY RETRIEVED", Color.GREEN)
                    if payload['result']:
                        c_print.myprint(f"FOUND {len(payload['result'])} DATASET", Color.GREEN)
                        return payload['result']
                    else:
                        c_print.myprint("NO DATASET FOUND", Color.RED)
                else:
                    c_print.myprint("CAN NOT RETRIEVE THE DATASET LIST", Color.RED)
            return []
        except requests.RequestException as e:
            raise DataRetrieverError(e)
        except Exception as e:
            raise DataRetrieverError(e)

    def get_dataset(self, dataset_id: str) -> dict:
        """
        :param dataset_id: the dataset ID
        :return: the dataset metadata
        """
        try:
            c_print.myprint(f"TRYING TO RETRIEVE DATASET {dataset_id} METADATA", Color.YELL)
            response = requests.get(f"{self.url}/api/3/action/package_show?id={dataset_id}")
            if response.status_code == 200:
                payload = response.json()
                if payload and payload['success']:
                    c_print.myprint("DATASET METADATA CORRECTLY RETRIEVED", Color.GREEN)
                    return payload
                else:
                    c_print.myprint("CAN NOT RETRIEVE THE DATASET METADATA", Color.RED)
            return {}
        except requests.RequestException as e:
            raise DataRetrieverError(e)
        except Exception as e:
            raise DataRetrieverError(e)


class DataRetrieverError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error happening to create, write or save the log
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")
