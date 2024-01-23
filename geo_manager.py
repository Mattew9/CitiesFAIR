import pandas as pd

from console_print import c_print, MyColors as Color


class CoorFinder:
    def __init__(self):
        """
        Create an object to decode IPA code in geospatial information
        """
        try:
            self.df = pd.read_csv("file/graphs/IPA_to_istat.csv", index_col=0, delimiter=',')
            self.df['Codice_IPA'] = self.df['Codice_IPA'].apply(lambda x: x.lstrip('0'))
            self.df['Codice_comune_ISTAT'] = self.df['Codice_comune_ISTAT'].apply(str).apply(lambda x: x.lstrip('0'))
            self.df1 = pd.read_csv("file/graphs/istat_to_coor.csv", delimiter=';')
            self.df1['istat'] = self.df1['istat'].apply(lambda x: x.lstrip('0'))
            self.df2 = pd.read_csv("file/graphs/istat_to_info.csv", delimiter=';')
            self.df2['istat'] = self.df1['istat'].apply(lambda x: x.lstrip('0'))
        except KeyError as e:
            raise CoordinateFinderError(e)
        except ValueError as e:
            raise CoordinateFinderError(e)
        except FileNotFoundError as e:
            raise CoordinateFinderError(e)
        except Exception as e:
            raise CoordinateFinderError(e)

    def get_location(self, ipa: str) -> tuple[int, int]:
        """
        Calculate the coordinates of an IPA
        :param ipa: IPA code of the PA
        :return: The coordinates of the IPA as (longitude, latitude)
        """
        try:
            ipa_ = ipa.lstrip('0')
            row = self.df[self.df['Codice_IPA'] == ipa_]
            if not row.empty:
                istat_cod = row.iloc[0]['Codice_comune_ISTAT']
                row1 = self.df1[self.df1['istat'] == istat_cod]
                if not row1.empty:
                    return row1.iloc[0]['lng'], row1.iloc[0]['lat']
            return 0, 0
        except KeyError as e:
            raise CoordinateFinderError(e)
        except ValueError as e:
            raise CoordinateFinderError(e)
        except FileNotFoundError as e:
            raise CoordinateFinderError(e)
        except Exception as e:
            raise CoordinateFinderError(e)

    def get_prov(self, ipa: str) -> str:
        """
        Calculates the province which the IPA belongs to
        :param ipa: IPA code of the PA
        :return: The province of the IPA
        """
        try:
            ipa_ = ipa.lstrip('0')
            row = self.df[self.df['Codice_IPA'] == ipa_]
            if not row.empty:
                istat_cod = row.iloc[0]['Codice_comune_ISTAT']
                row1 = self.df2[self.df1['istat'] == istat_cod]
                if not row1.empty:
                    return row1.iloc[0]['provincia']
            return ''
        except KeyError as e:
            raise CoordinateFinderError(e)
        except ValueError as e:
            raise CoordinateFinderError(e)
        except FileNotFoundError as e:
            raise CoordinateFinderError(e)
        except Exception as e:
            raise CoordinateFinderError(e)

    def get_city(self, ipa: str) -> str:
        """
        Calculates the city which the IPA belongs to
        :param ipa: IPA code of the PA
        :return: The city of the IPA
        """
        try:
            ipa_ = ipa.lstrip('0')
            row = self.df[self.df['Codice_IPA'] == ipa_]
            if not row.empty:
                istat_cod = row.iloc[0]['Codice_comune_ISTAT']
                row1 = self.df2[self.df1['istat'] == istat_cod]
                if not row1.empty:
                    return row1.iloc[0]['comune']
            return ''
        except KeyError as e:
            raise CoordinateFinderError(e)
        except ValueError as e:
            raise CoordinateFinderError(e)
        except FileNotFoundError as e:
            raise CoordinateFinderError(e)
        except Exception as e:
            raise CoordinateFinderError(e)


class CoordinateFinderError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error calculating geospatial information
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")
