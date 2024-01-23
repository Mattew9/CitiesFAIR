from abc import abstractmethod, ABC

from console_print import c_print, MyColors as Color


class TestError(Exception):
    """Exception raised from error happening while running a test

    Attributes:
    test - the test running while the error was raised
    error - error captured
    """
    def __init__(self, test: str, error: Exception):
        self.test = test
        self.error = str(error)
        c_print.myprint(f"ERROR DURING TEST {self.test} -> {self.error}", Color.RED, 2)
        super().__init__("ERROR DURING TEST -> " + self.test + " -> " + self.error)


class Metric(ABC):
    def __init__(self, name: str, goal: str):
        """Create a test for a metric specified in name. Name should be in uppercase (ex. F1).
        The goal is a string explicating what the test is looking for in the (meta)data"""
        self.name = name
        self.goal = goal
        self.max_point = 1
        self.scored_point = 0


    def start_test(self) -> None:
        self.max_point = 1
        self.scored_point = 0
        """Print the starting test info(ex. F1 -> GOAL OF METRIC F1"""
        c_print.myprint(f"{self.name}->{self.goal}", Color.CYAN, 0)

    def end_test(self) -> float:
        """Print the ending test info(ex. F1 -> max point: 1 | point scored: 0"""
        c_print.myprint(f"{self.name} -> max point = {self.max_point} | point scored = {self.scored_point}", Color.UNDERLINE, 0)
        return self.scored_point/self.max_point * 100

    def hint_test(self, hint: str) -> None:
        """To use if (meta)data lack something a test is searching for.
        It prints a line to inform the user of what s lacking and send a copy to the reporter if a report is needed"""
        c_print.myprint(f"{self.name}-> {hint}", Color.BLUE, 3)

    @staticmethod
    def info_test(info: str, grade: int) -> None:
        """To use inside a running test to explicit single steps
        Attributes
        info - information about the step considered
        grade - the grade of depth of the step (if a step is inside a step is a 2nd grade step)
        """
        c_print.myprint("*" * grade + info, Color.ENDC, 0)

    @abstractmethod
    def run_test(self) -> tuple[float, float]:
        """TO IMPLEMENT
        the assessment function for the metrics"""



class Principle(ABC):
    def __init__(self, name, metric_tests: list[Metric]):
        self.name = name
        self.metric_tests = metric_tests

    def run(self) -> dict[str: tuple[float, float]]:
        results = {}
        for test in self.metric_tests:
            try:
                results[test.name] = test.run_test()
            except TestError:
                results[test.name] = None
            except Exception as e:
                results[test.name] = None
                c_print.myprint(f"UNEXPECTED ERROR IN {test.name} -> {str(e)}")

        return results


class Structure(ABC):
    def __init__(self, name, principles:  list[Principle]):
        self.name = name
        self.principles = principles

    def run(self):
        results = {}
        for principle in self.principles:
            results[principle.name] = principle.run()
            principle.run()

        return results
