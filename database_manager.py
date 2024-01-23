import datetime
from typing import Optional
import mysql.connector

from console_print import c_print, MyColors as Color


class DatabaseManager:
    def __init__(self, endpoint, user, password, port, name, tool_v):
        self.endpoint = endpoint
        self.user = user
        self.password = password
        self.port = port
        self.name = name
        self.tool_v = tool_v
        self.portal_id = None

        self.my_database = mysql.connector.connect(
            host=self.endpoint,
            port=self.port,
            user=self.user,
            passwd=self.password,
            database=self.name,
            autocommit=False
        )

    def set_portal_id(self, portal_id: str) -> None:
        """
        Set the portal id for the environment to use in the queries.
        :param portal_id: The id of the portal to use
        :return: None
        """
        self.portal_id = portal_id

    def get_portal_list(self) -> list:
        """
        :return: An array with all the portals already existing in the database
        """
        with self.my_database.cursor() as cursor:
            query = """SELECT idPortal
                        FROM Portal"""
            cursor.execute(query, )
            results = cursor.fetchall()

        portal_list = []
        for holder in results:
            portal_list.append(holder[0])

        cursor.close()
        return portal_list

    def save_portal(self, portal_id: str, portal_name: str = '') -> None:
        """
        Save a new instance of portal in the database
        :return: None
        """
        with self.my_database.cursor() as cursor:
            query = """INSERT INTO Portal
                        (idPortal, namePortal)
                        VALUES (%s, %s)"""
            cursor.execute(query, (portal_id, portal_name))
        self.my_database.commit()

    def get_dataset_list(self) -> list:
        """
        :return: A list of all the dataset ids already in the database for the chosen portal
        """
        with self.my_database.cursor() as cursor:
            query = """SELECT idDataset
                    FROM Dataset
                    WHERE portalDataset = %s"""
            cursor.execute(query, (self.portal_id,))
            results = cursor.fetchall()

        dataset_list = []
        for dataset in results:
            dataset_list.append(dataset[0])
        return dataset_list

    def get_holders_list(self) -> list:
        """
        :return: A list of all the holders ids already in the database for the chosen portal
        """
        with self.my_database.cursor() as cursor:
            query = """SELECT idHolder
                        FROM Holder
                        WHERE portalHolder = %s"""
            cursor.execute(query, (self.portal_id,))
            results = cursor.fetchall()

        holder_list = []
        for holder in results:
            holder_list.append(holder[0])

        cursor.close()
        return holder_list

    def get_category_list(self) -> list:
        """
        :return: A list of all the categories ids already in the database
        """
        with self.my_database.cursor() as cursor:
            query = """SELECT idCategory
                    FROM Category"""

            cursor.execute(query, ())
            results = cursor.fetchall()

        category_list = []
        for category in results:
            category_list.append(category[0])

        return category_list

    def save_holder(self, holder_id: str, holder_name: str,
                    coor: Optional[tuple], prov: Optional[str], com: Optional[str]) -> None:
        """
        Save a new holder into the database
        :param holder_id: the id of the new holder
        :param holder_name: the name of the new holder
        :param coor: the geo coordinates of the holder as a tuple latitude, longitude
        :param prov: the province of the holder
        :param com: the city of the holder
        :return: None
        """
        with self.my_database.cursor() as cursor:
            query = """INSERT INTO Holder
                        (idHolder, portalHolder, nameHolder, latitude, longitude, com, prov)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(query, (holder_id, self.portal_id, holder_name,
                                   (coor[0] if coor else None), (coor[1] if coor else None), prov, com))
        self.my_database.commit()

    def save_dataset(self, dataset_id: str, dataset_holder: str,
                     accrual_periodicity: Optional[int], dataset_name: Optional[str] = None) -> None:
        """
        Save a new dataset in the database
        :param dataset_id: the id of the dataset
        :param dataset_holder:  the holder of the dataset
        :param accrual_periodicity: the accrual periodicity of the dataset
        :param dataset_name: the name of the dataset
        :return: None
        """
        with self.my_database.cursor() as cursor:
            query = """INSERT INTO Dataset
                    (idDataset, portalDataset, holderDataset, nameDataset, accPeriodicity)
                    VALUES (%s, %s, %s, %s, %s)"""
            cursor.execute(query, (dataset_id, self.portal_id, dataset_holder, dataset_name, accrual_periodicity))

        self.my_database.commit()

    def get_category_for_dataset(self, dataset_id: str):
        """
        :param dataset_id:  the id of the dataset searching the categories for
        :return: A list of all the categories ids for a specific dataset
        """
        with self.my_database.cursor() as cursor:
            query = """SELECT idCategory
                    FROM DatasetCategory
                    WHERE idDataset = %s"""

            cursor.execute(query, (dataset_id,))
            results = cursor.fetchall()

        category_list = []
        for category in results:
            category_list.append(category[0])

        return category_list

    def save_category_dataset_relation(self, dataset_id: str, category_id: str) -> None:
        """
        Save a new relation between a dataset and a category
        :param dataset_id: the id of the dataset
        :param category_id: the category to link to the dataset
        :return: None
        """
        with self.my_database.cursor() as cursor:
            sql_query = """
            INSERT INTO DatasetCategory 
            (idDataset, idCategory)
            VALUES (%s, %s)"""

            cursor.execute(sql_query, (dataset_id, category_id))

    def new_assessment(self, dataset_id: str) -> int:
        """
        Create a new assessment instance in the database
        :param dataset_id: the id of the dataset assessed
        :return: The id of the assessment for the dataset
        """

        with self.my_database.cursor() as cursor:
            sql_query = """
            INSERT INTO Assessment 
            (idDataset, datetime, toolVersion)
            VALUES (%s, %s, %s)"""

            cursor.execute(sql_query, (dataset_id, datetime.datetime.now(), self.tool_v))

            assessment_id = cursor.lastrowid

        self.my_database.commit()

        return assessment_id

    def save_metadata_assessment(self, assessment_id: int, result: dict,
                                 is_modified: Optional[bool], file_id: Optional[str]) -> None:
        """
        Save a new metadata assessment in the database
        :param assessment_id: the id of the assessment made
        :param result: the results of the assessment
        :param is_modified: if the metadata has changed since last evaluation
        :param file_id: the id of the file backup in the cloud for the metadata if present
        :return: None
        """
        val = [assessment_id, file_id]

        for principle in result:
            for metric in result[principle]:
                val.append(result[principle][metric])

        val.append(is_modified)

        with self.my_database.cursor() as cursor:
            sql_query = """
            INSERT INTO MetadataAssessment 
            (idAssessment, idFileCloud,
            F1, F2, F3, F4, G_F, 
            A1, `A1_1`, `A1_2`, A2, G_A, 
            I1, I2, I3, G_I, 
            `R1_1`, `R1_2`, `R1_3`, G_R, 
            isModified)
            VALUES (%s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, 
            %s, %s, %s)"""

            cursor.execute(sql_query, tuple(val))

        self.my_database.commit()

    def save_data_assessment(self, assessment_id: int, resource_id: str, resource_name: str, result: dict,
                             is_modified: Optional[bool], file_id: Optional[str]) -> None:
        """
        Save a new metadata assessment in the database
        :param assessment_id: the id of the assessment made
        :param resource_id: the id of the resource as in the metadata
        :param resource_name: the name of the resource as in the metadata
        :param result: the results of the assessment
        :param is_modified: if the metadata has changed since last evaluation
        :param file_id: the id of the file backup in the cloud for the metadata if present
        :param result:
        :return:
        """
        val = [assessment_id, file_id, resource_id, resource_name]

        for principle in result:
            for metric in result[principle]:
                val.append(result[principle][metric])

        val.append(is_modified)

        with self.my_database.cursor() as cursor:
            sql_query = """INSERT INTO DataAssessment
                (idAssessment, idFileCloud, idResource, nameResource,
                F1, F2, F3, F4, G_F, 
                A1, `A1_1`, `A1_2`, A2, G_A, 
                I1, I2, I3, G_I, 
                `R1_1`, `R1_2`, `R1_3`, G_R, 
                isModified)
                VALUES (%s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s,
                %s, %s, %s, %s, 
                %s)"""

            cursor.execute(sql_query, tuple(val))

        self.my_database.commit()

    def save_category(self, category: str) -> None:
        """
        Save a new category into the database
        :param category: the category id
        :return: None
        """
        with self.my_database.cursor() as cursor:
            query = """INSERT INTO Category
                    (idCategory)
                    VALUES (%s)"""
            cursor.execute(query, (category,))

        self.my_database.commit()

    def get_expired_datasets_list(self):
        """
        :return: A list with all the ids of the datasets which last evaluation was done before its accrual periodicity
        """
        with self.my_database.cursor() as cursor:
            query = """SELECT Dataset.idDataset, Assessment.datetime
                        FROM Dataset
                        INNER JOIN (
                            SELECT idDataset, MAX(datetime) AS last_assessment
                            FROM Assessment
                            GROUP BY idDataset) last_assessments
                        ON Dataset.idDataset = last_assessments.idDataset
                        INNER JOIN Assessment
                        ON Dataset.idDataset = Assessment.idDataset AND Assessment.datetime = last_assessments.last_assessment
                        WHERE Dataset.accPeriodicity < DATEDIFF(CURDATE(), Assessment.datetime)
                        AND Dataset.portalDataset = %s"""

            cursor.execute(query, (self.portal_id,))
            results = cursor.fetchall()

        dataset_list = []
        for dataset in results:
            dataset_list.append(dataset[0])
        return dataset_list

class DatabaseError(Exception):
    def __init__(self, capt_error: Exception):
        """
        Exception raised from error while operating with the database
        :param capt_error:  error captured
        """
        import inspect

        self.capt_error = str(capt_error)
        # Get the name of the function which called the error
        self.caller_function = inspect.currentframe().f_back.f_code.co_name
        c_print.myprint(f"ERROR DURING {self.caller_function}  -> {self.capt_error}", Color.RED, 2)
        super().__init__(f"ERROR DURING {self.caller_function}  -> {self.capt_error}")
