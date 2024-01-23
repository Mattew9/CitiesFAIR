import tkinter as tk
from tkinter import messagebox
import datetime
import inspect
from typing import Union

import constants
import log


class MyColors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELL = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    def __add__(self, other):
        return self + other


class ConsolePrint:
    _instance = None

    def __new__(cls):  # SINGLETON
        if cls._instance is None:
            cls._instance = super(ConsolePrint, cls).__new__(cls)
        return cls._instance

    def __init__(self, to_report: bool = False):
        self.my_log = log.MyLogger()
        self.my_report = None
        if to_report:
            self.my_report = log.MyReporter()

    def myprint(self, text: Union[str, Exception], color: MyColors = MyColors.ENDC, to_log: int = 1,
                test_mode: bool = False) -> None:
        """
        Function to print to console with a formatted style, it also includes the logging system from MyLogger
        :param text: text to print
        :param color: color of choice for the text
        :param to_log: the level of log: 0 - not to log | 1 - to log | 2 - to log as error | 3 - to log as report
        :param test_mode: to use when not print in console while testing
        :return: None
        """
        if test_mode:
            pass
        else:
            border = int((constants.Constants.STR_LUN - len(text)) / 2)
            print(color + '-' * border + text + '-' * border + MyColors.ENDC)

        if to_log == 1:
            self.my_log.to_log(str(datetime.datetime.now()) + ': ' + text)
        elif to_log == 2:
            self.my_log.to_error(str(datetime.datetime.now()) + ': ' + text,
                                 inspect.getouterframes(inspect.currentframe(), 2))
        elif to_log == 3 and self.my_report:
            if self.my_report:
                self.my_report.to_report(text)

    @staticmethod
    def aline(color: MyColors = MyColors.ENDC):
        """
        Create a simple line full of - in the console to separate div
        :param color:
        :return:
        """
        print(color + "-" * constants.Constants.STR_LUN)

    @staticmethod
    def astar(color=MyColors.ENDC):
        """
        Create a simple line full of * in the console to separate div
        :param color:
        :return:
        """
        print(color + "*" * constants.Constants.STR_LUN)


def box(title: str, text: str, _icon: str = 'warning') -> int:
    """
    Simple dialog box
    :param title: Title of the dialog box
    :param text: Text of the dialog box
    :param _icon: icon of the dialog box
    :return: the answer (yes as 1 no as 0)
    """
    msg_box = tk.messagebox.askquestion(title, text, icon=_icon)
    if msg_box == 'yes':
        ans = 1
    else:
        ans = 0
    return ans


c_print = ConsolePrint()  # console printer
