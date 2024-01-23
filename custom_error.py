import inspect

from console_print import c_print, MyColors as Color


class CustomError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error happening to create, write or save the log
        :param capt_error:  error captured
        """
        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")
