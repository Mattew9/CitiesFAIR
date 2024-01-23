import json
import os
from typing import Union
import pandas as pd
from rdflib import Graph

from console_print import c_print, MyColors as Color


generic_file_path = 'file/'
temp_file_path = 'temp/'
settings_file_path = 'settings/'


class FileManager:
    _instance = None

    def __new__(cls):  # SINGLETON
        if cls._instance is None:
            cls._instance = super(FileManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.file_path = generic_file_path
        self.temp_file_path = temp_file_path

    def delete_file(self, file_name: str, f_type: str = 'txt') -> None:
        """
        Delete a file from the temp folder
        :param file_name: the name of the file to delete,
                          if f_type it's not the declared f_name it's expected to have format included
        :param f_type: the format of the file to delete, default: None
        :return: None
        """
        try:
            if f_type:
                os.remove(self.file_path + self.temp_file_path + file_name + '.' + f_type)
            else:
                os.remove(self.file_path +  self.temp_file_path + file_name)
            c_print.myprint(f"File {self.file_path +  self.temp_file_path + file_name + '.' + f_type} has been deleted",
                            Color.GREEN)
        except OSError as e:
            raise FileManagerError(e)
        except Exception as e:
            raise FileManagerError(e)

    def open_file(self, f_name: str, f_form: str, to_json: bool = False, k_path: str = '', path: str = '') \
            -> Union[dict, str, pd.DataFrame, Graph]:
        """
        Open a file from a specific path
        :param f_name: name of the file to open
        :param f_form: format of the file to open
        :param to_json: if true the file content is converted in a json dictionary
        :param k_path: known path: 'set' for settings file | 'temp' for temporary file
        :param path: path of the file to open, if not declared the default file path is used
        :return: an object coherent with the file: str for text | dictionary for json | dataframe for csv | graph for rdf
        """
        try:
            known = ''
            if k_path == 'set':
                known = settings_file_path
            elif k_path == 'temp':
                known = temp_file_path
            with open(self.file_path + path + known + f_name + '.' + f_form, "r") as file:
                if (f_form == 'json' or f_form == 'txt') and to_json:
                    content = json.loads(file.read())
                    return content
                elif (f_form == 'json' or f_form == 'txt') and not to_json:
                    content = file.read()
                    return content
                elif f_form == 'csv':
                    df = pd.read_csv(file)
                    return df
                elif f_form == 'rdf':
                    g = Graph()
                    g.parse(self.file_path + path + known + f_name + '.' + f_form, format='xml')
                    return g

        except FileNotFoundError as e:
            raise FileManagerError(e)
        except IOError as e:
            raise FileManagerError(e)
        except Exception as e:
            raise FileManagerError(e)

    def write_file(self, f_name_w_form: str, to_write: str, path: str = '', to_dump: bool = None):
        """
        Write a new file
        :param f_name_w_form: the name of the file to write with its format
        :param to_write: the content of the file
        :param path: path to save the file, if not declare the default file path is used
        :param to_dump: to use for json dictionary, if True the dictionary is dumped before write
        :return: None
        """
        try:
            if to_dump:
                to_write_ = json.dumps(to_write)
            else:
                to_write_ = to_write

            with open(self.file_path + path + f_name_w_form, "w") as file:
                file.write(to_write_)
            return

        except FileNotFoundError as e:
            raise FileManagerError(e)
        except IOError as e:
            raise FileManagerError(e)
        except Exception as e:
            raise FileManagerError(e)


class FileManagerError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error happening to write, delete or open a file
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")


file_manager = FileManager()
