import csv

from file_manager import temp_file_path, generic_file_path
from console_print import c_print, MyColors as Color


class CsvTester:
    def __init__(self):
        self.file_path = generic_file_path + temp_file_path

    def check_delimiter(self, file_name: str) -> bool:
        """
        Check the delimiter of a csv file
        :param file_name: the name of the file
        :return: True if delimiter is ; False otherwise
        """
        try:
            with open(self.file_path + file_name) as f:
                delimiter = str(csv.Sniffer().sniff(f.read()).delimiter)
            if delimiter == ';':
                return True
            return False
        except Exception as e:
            raise CsvTesterError(e)

    # TEST BASED ON PROBABILITY .
    # In a properly formatted CSV file, each row should have the same number of commas since
    # each row represents a row in the table. If there are multiple tables, it's likely that there will be a different
    # number of columns in these tables, which will result in a different number of commas in the first few lines.

    def is_single_table_csv(self, file_name: str) -> bool:
        """
        Check if the csv file contains only a table or more
        :param file_name: the name of the file
        :return: True if the file contains a single table, False otherwise
        """
        try:
            with open(self.file_path + file_name, "r") as f:
                delimiter = csv.Sniffer().sniff(f.readline()).delimiter
                f.seek(0)

                lines = csv.reader(f, delimiter=delimiter)
                for idx, line in enumerate(lines):
                    if idx == 0:
                        num_col_first_row = len(line)
                    elif line and len(line) != num_col_first_row:
                        return False
                return True
        except Exception as e:
            raise CsvTesterError(e)

    def is_blank_free(self, file_name: str) -> bool:
        """
        Check if a csv contains blank fields
        :param file_name: the name of the file
        :return: True if the csv is blank free, false otherwise
        """
        try:
            with open(self.file_path + file_name) as f:
                file_content = f.read()
            lines = file_content.split('\n')
            for idx, line in enumerate(lines):
                if not line.strip() and not idx == len(lines) - 1:
                    return False
            return True
        except Exception as e:
            raise CsvTesterError(e)

    # BASED ON PROBABILITY
    def has_header(self, file_name: str) -> bool:
        """
        Check if a csv contains header
        :param file_name: the name of the file
        :return: True if the csv is header free
        """
        try:
            header = []
            with open(self.file_path + file_name, 'r') as file:
                delimiter = csv.Sniffer().sniff(file.read()).delimiter
                file.seek(0)

                csv_reader = csv.reader(file, delimiter=delimiter)
                while not header:
                    header = next(csv_reader, None)

            count = 0
            cell_count = 0
            print(header)
            if header is not None:
                for cell in header:
                    cell_count += 1
                    for char in cell:
                        if char.isalpha():
                            count += 1
                            break
            if count == cell_count:
                return True
            return False
        except Exception as e:
            raise CsvTesterError(e)


class CsvTesterError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error evaluating a csv file
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")


sp_csv = CsvTester()
