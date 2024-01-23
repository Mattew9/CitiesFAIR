import re
import xml.etree.ElementTree as Et

from file_manager import temp_file_path, generic_file_path
from console_print import c_print, MyColors as Color


class XmlTester:
    def __init__(self):
        self.file_path = generic_file_path + temp_file_path

    def has_declaration(self, file_name: str) -> bool:
        """
        Check if the xml as a declaration header
        :param file_name: the name of the file
        :return: True if xml contains header, False otherwise
        """
        try:
            with open(self.file_path + file_name, 'r') as file:
                first_line = file.readline().strip()
                return first_line.startswith('<?xml')
        except Exception as e:
            raise XmlTesterError(e)

    def check_escape_use(self, file_name: str) -> bool:
        """
        Check for the correct use of the escape characters inside the xml
        :param file_name: the name of the file
        :return: True if escape are used correctly, False otherwise
        """
        try:
            with open(self.file_path + file_name, 'r') as f:
                file_content = f.read()

            special_characters = {
                '&': '&amp;',
                '<': '&lt;',
                '>': '&gt;',
                '"': '&quot;',
                "'": '&apos;'
            }

            def find_all_occurrences(strings, text_):
                occurrence_indices = []
                for string in strings:
                    start = 0
                    while True:
                        index = text_.find(string, start)
                        if index == -1:
                            break
                        occurrence_indices.append(index)
                        start = index + 1
                return occurrence_indices

            lines = file_content.split('\n')
            pattern = re.compile(r'<[A-Za-z0-9]+>([\S\s]+)</[A-Za-z0-9]+>')
            for line in lines:
                occ = re.findall(pattern, line)
                if occ:
                    text = occ[0]
                    for char in special_characters.keys():
                        if char == '&':
                            if (text.find(char) != -1 and
                                    text.find(char) not in find_all_occurrences(special_characters.values(), text)):
                                return False
                        elif text.find(char) != -1:
                            return False
            return True
        except Exception as e:
            raise XmlTesterError(e)

    def check_no_program_info(self, file_name: str) -> bool:
        """
        Check if the xml has a declaration about the program used to build the xml
        :param file_name: the name of the file
        :return: True if no element about the program used are found, False otherwise
        """
        try:
            tree = Et.parse(self.file_path + file_name)
            root = tree.getroot()

            def has_program_info_elements(element):
                program_info_elements = ["software", "createdWith", "createdwith"]
                for program_info_element in program_info_elements:
                    if element.find(program_info_element) is not None:
                        return True
                return False

            if has_program_info_elements(root):
                return False
            else:
                return True
        except Exception as e:
            raise XmlTesterError(e)


class XmlTesterError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error evaluating a xml file
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")


sp_xml = XmlTester()
