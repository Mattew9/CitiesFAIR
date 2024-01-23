import json
import os
import sys
import time
from dotenv import load_dotenv

import database_manager
import gui
import cloud_manager
import geo_manager
from console_print import c_print, MyColors as Color
from event_bus import event_bus


class Loader:
    def __init__(self, use_cloud: bool, use_geo: bool):
        self.t_version = None
        self.db_endpoint = None
        self.db_user = None
        self.db_password = None
        self.db_port = None
        self.db_name = None
        self.c_folder_id = None
        self.c_service_account = None
        self.cloud_service = None
        self.geo_service = None
        self.database = None
        self.use_cloud = use_cloud
        self.use_geo = use_geo

    def load_system_for_portal(self) -> None:
        """
        Load all the environment variables and services chosen necessary for the evaluation
        :return:
        """
        try:
            c_print.myprint("LOADING ENVIRONMENT VARIABLES", Color.YELL)
            if load_dotenv():
                self.load_env()
                c_print.myprint("ENVIRONMENT VARIABLES LOADED", Color.GREEN)
            else:
                raise LoaderFatalError

            # Loading for cloud services
            try:
                if self.use_cloud:
                    c_print.myprint("LOADING CLOUD SERVICES", Color.YELL)
                    self.load_cloud()
                    c_print.myprint("CLOUD SERVICE LOADED", Color.GREEN)
            except LoaderError:
                c_print.myprint("ERROR LOADING CLOUD SERVICES", Color.RED)
                if gui.box("ERRORE NEL CARICAMENTO DEL CLOUD",
                           "Impossibile caricare i servizi cloud, controllare le variabili di ambiente. "
                           "Vuoi continuare senza utilizzare i servizi cloud?"):
                    c_print.myprint("CLOUD SERVICES BYPASSED", Color.GREEN)
                else:
                    self.force_close()
                    sys.exit()

            # Loading for geo services
            try:
                if self.use_geo:
                    c_print.myprint("LOADING GEO SERVICES", Color.YELL)
                    self.load_geo()
                    c_print.myprint("CLOUD SERVICE LOADED", Color.GREEN)
            except LoaderError:
                c_print.myprint("ERROR LOADING GEO SERVICES", Color.RED)
                if gui.box("ERRORE NEL CARICAMENTO DEL LOCALIZZATORE",
                           "Impossibile caricare i servizi di localizzazione."
                           "Vuoi continuare senza utilizzare i servizi di localizzazione?"):
                    c_print.myprint("GEO SERVICES BYPASSED", Color.GREEN)
                else:
                    self.force_close()
                    sys.exit()

            # Loading database
            c_print.myprint("LOADING DATABASE", Color.YELL)
            self.load_database()
            c_print.myprint("DATABASE LOADED", Color.GREEN)

        except LoaderFatalError as e:
            c_print.myprint(f"ERROR: cannot load environment variables ->  {e}", Color.RED, 2)
            sys.exit()
        except Exception as e:
            c_print.myprint(f"ERROR: cannot load environment variables -> {e}", Color.RED, 2)
            sys.exit()

    def load_env(self):
        """
        Load the basic environment variables
        :return:
        """
        try:
            self.t_version = os.getenv("TOOL_VERSION")
            self.db_endpoint = os.getenv("DATABASE_ENDPOINT")
            self.db_user = os.getenv("DATABASE_USER")
            self.db_password = os.getenv("DATABASE_PASSWORD")
            self.db_port = os.getenv("DATABASE_PORT")
            self.db_name = os.getenv("DATABASE_NAME")
        except FileNotFoundError as e:
            raise LoaderFatalError(e)
        except SyntaxError as e:
            raise LoaderFatalError(e)
        except Exception as e:
            raise LoaderFatalError(e)

    def load_cloud(self):
        try:
            self.c_folder_id = os.getenv("CLOUD_FOLDER_ID")
            self.c_service_account = json.loads(os.getenv("CLOUD_SERVICE_ACCOUNT"))
            c_print.myprint("LOADING CLOUD SERVICES", Color.YELL)
            try:
                self.cloud_service = cloud_manager.Cloud(self.c_folder_id, self.c_service_account)
                c_print.myprint("CLOUD SERVICES LOADED", Color.GREEN)
            except cloud_manager.CloudError as e:
                raise LoaderError(e)
        except FileNotFoundError as e:
            raise LoaderError(e)
        except SyntaxError as e:
            raise LoaderError(e)
        except Exception as e:
            raise LoaderError(e)

    def load_geo(self):
        try:
            self.geo_service = geo_manager.CoorFinder()
        except geo_manager.CoordinateFinderError as e:
            raise LoaderError(e)

    def load_database(self):
        try:
            self.database = database_manager.DatabaseManager(self.db_endpoint, self.db_user, self.db_password,
                                                             self.db_port, self.db_name, self.t_version)
        except database_manager.DatabaseError:
            raise LoaderFatalError

    def get_cloud(self) -> cloud_manager.Cloud:
        return self.cloud_service

    def get_geo(self) -> geo_manager.CoorFinder:
        return self.geo_service

    def get_database(self) -> database_manager.DatabaseManager:
        return self.database

    @staticmethod
    def force_close():
        print("CLOSING IN ", end="")
        t = 10
        while t:
            t_min, t_sec = divmod(t - 1, 60)
            timer = '{:02d}:{:02d}'.format(t_min, t_sec)
            print("\r", "CLOSING IN " + timer, end="")
            time.sleep(1)
            t -= 1
        event_bus.publish({'type': 'forced_closure'})


class LoaderError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error happening loading the tool
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")


class LoaderFatalError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error happening loading the tool
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")
