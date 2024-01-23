import re

from file_manager import temp_file_path, generic_file_path
from console_print import c_print, MyColors as Color


class JsonTester:
    def __init__(self):
        self.file_path = generic_file_path + temp_file_path

    def validate_var_type(self, file_name: str) -> bool:
        """
        Check if the variable in the JSON use the correct type.
        EX. int should be saved as int like 111, not string '111'
        :param file_name: the name of the file
        :return: True if types are used correctly, False otherwise
        """
        try:
            with open(self.file_path + file_name) as f:
                file_content = f.read()
            pattern1 = re.compile(r':\s"[0-9]+"', re.IGNORECASE)
            pattern2 = re.compile(r':\s"True"', re.IGNORECASE)
            pattern3 = re.compile(r':\s"False"', re.IGNORECASE)
            lines = file_content.split('\n')
            for line in lines:
                if re.findall(pattern1, line) or re.findall(pattern2, line) or re.findall(pattern3, line):
                    return False
            return True
        except Exception as e:
            raise JsonTesterError(e)


class JsonTesterError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error evaluating a json file
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")


sp_json = JsonTester()
