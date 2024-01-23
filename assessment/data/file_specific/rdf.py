from file_manager import temp_file_path, generic_file_path
from console_print import c_print, MyColors as Color


class RdfTester:
    def __init__(self):
        self.file_path = generic_file_path + temp_file_path

    def validate_utf8(self, file_name: str) -> bool:
        """
        Check gor utf-8 validation for the file
        :param file_name: name of the file with extension
        :return: True if the utf-8 validation pass, False otherwise
        """
        try:
            with open(self.file_path + file_name, "r", encoding="utf-8") as f:
                content = f.read()
            return True
        except UnicodeDecodeError:
            return False
        except FileNotFoundError as e:
            raise RdfTesterError(e)
        except Exception as e:
            raise RdfTesterError(e)


class RdfTesterError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error evaluating a rdf file
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")


sp_rdf = RdfTester()
