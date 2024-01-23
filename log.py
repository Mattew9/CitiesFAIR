import datetime
import inspect
from typing import Any, Union
from tkinter import filedialog


class MyLogger:
    """
    Class to build a log system
    """
    def __init__(self):
        self.logs_path = 'logs/log.txt'

    def to_log(self, to_log: Any, first: bool = True) -> None:
        """
        Simple log function

        :param to_log: the string or element to log
        :param first: if true create a simple div line to distinguish different session,
        used only at the beginning of the session
        :return: None
        """
        try:
            if first:
                with open(self.logs_path, "a+") as file:
                    file.writelines(
                        ["--------------------------------------------------------------------------------------\n",
                         str(datetime.datetime.now().time()), "--> ", str(to_log), "\n"])
            else:
                with open("logs/log.txt", "a+") as file:
                    file.writelines(["----",
                                     str(datetime.datetime.now().time()), "--> ", str(to_log), "\n"])
        except FileNotFoundError as e:
            raise LogError(e)
        except Exception as e:
            raise LogError(e)

    def clean_stack(self):
        """
        Clean the log file
        :return: None
        """
        try:
            with open(self.logs_path, "w"):
                pass

        except FileNotFoundError as e:
            raise LogError(e)
        except Exception as e:
            raise LogError(e)

    def to_error(self, to_error: str, frame: Union[inspect.FrameInfo, list[inspect.FrameInfo]]) -> None:
        """
        Log function to log error in error log file

        :param to_error: error to log
        :param frame: the frame which call for the error
        :return: None
        """
        try:
            with open(self.logs_path, "a+") as file:
                file.writelines(["---------------------------------------------------------------------------------\n",
                                 str(datetime.datetime.now()), "\n",
                                 str(to_error), "\n",
                                 str(frame), "\n"])

        except FileNotFoundError as e:
            raise LogError(e)
        except Exception as e:
            raise LogError(e)


class MyReporter:
    """
    Class to build a reporter system which allow the user to have a detailed report of the dataset evaluations
    """
    def __init__(self):
        self.report = ''
        self.report_path = ''
        self.report += "euFAIR - THE TOOL FOR YOUR EU ORIENTED FAIRIFICATION\n"

    def to_report(self, to_report: str) -> None:
        """
        Add an unsatisfied requirements in the dataset (meta)data to the report
        :param to_report: unsatisfied requirements to report
        :return: None
        """
        self.report += (to_report + "\n")

    def save_report(self) -> None:
        """
        Save the report as a txt file
        :return: None
        """
        try:
            file_path = filedialog.asksaveasfilename(
                title="Salva il report",
                filetypes=[("File di testo.txt", "*.txt"), ("Tutti i file", "*.*")]
            )

            if file_path:
                if not file_path.endswith(".txt"):
                    file_path += ".txt"
                with open(file_path, 'w') as file:
                    file.write(self.report)

        except IOError as e:
            raise LogError(e)
        except Exception as e:
            raise LogError(e)


class LogError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error happening to create, write or save the log
        :param capt_error:  error captured
        """
        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        print(f"NOT LOGGABLE ERROR DURING {self.caller_function}  -> {self.capt_error}")
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")
