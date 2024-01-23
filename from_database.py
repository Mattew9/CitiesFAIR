import csv
import datetime

import mysql.connector
import os
from dotenv import load_dotenv


class DatabaseManager:
    def __init__(self):
        if load_dotenv():
            self.t_version = os.getenv("TOOL_VERSION")
            self.endpoint = os.getenv("DATABASE_ENDPOINT")
            self.user = os.getenv("DATABASE_USER")
            self.password = os.getenv("DATABASE_PASSWORD")
            self.port = os.getenv("DATABASE_PORT")
            self.name = os.getenv("DATABASE_NAME")

        self.my_database = mysql.connector.connect(
            host=self.endpoint,
            port=self.port,
            user=self.user,
            passwd=self.password,
            database=self.name,
            autocommit=False
        )
        principi_metriche = [('F', 'F1'), ('F', 'F2'), ('F', 'F3'), ('F', 'F4'), ('F', 'G_F'),
                              ('A', 'A1'), ('A', 'A1_1'), ('A', 'A1_2'), ('A', 'A2'), ('A', 'G_A'),
                              ('I', 'I1'), ('I', 'I2'), ('I', 'I3'), ('I', 'G_I'),
                              ('R', 'R1_1'), ('R', 'R1_2'), ('R', 'R1_3'), ('R', 'G_R')]

        cursor = self.my_database.cursor()

        cursor.execute("""SELECT idDataset, holderDataset FROM Dataset""")

        datasets = cursor.fetchall()

        with open('assets/prova.csv', 'w', newline='') as file_csv:
            colonne = ['holder', 'dataset_id', 'METRICA', 'PRINCIPIO', 'punteggio', 'PROVINCIA', 'CATEGORIA']
            writer = csv.writer(file_csv)
            writer.writerow(colonne)

            for dataset, holder in datasets:
                cursor.execute("""SELECT nameHolder FROM Holder WHERE idHolder = %s""", (holder, ))
                holder_name = cursor.fetchall()[0][0]

                cursor.execute("""SELECT idAssessment FROM Assessment WHERE idDataset = %s ORDER BY datetime DESC 
                LIMIT 1""",
                               (dataset, ))

                last_assessment_ = cursor.fetchall()
                if last_assessment_:
                    last_assessment = last_assessment_[0][0]

                    for principle, metric in principi_metriche:
                        cursor.execute(f"""SELECT IFNULL(AVG({metric}), 0)
                        FROM DataAssessment d
                        WHERE idAssessment = %s""", (last_assessment,))

                        average_ = cursor.fetchall()
                        if average_:
                            average = average_[0][0]
                        else:
                            average = 0
                        cursor.execute(f"""SELECT IFNULL({metric}, 0) 
                        FROM MetadataAssessment 
                        WHERE idAssessment = %s""",
                                       (last_assessment, ))
                        average2_ =  cursor.fetchall()
                        if average2_:
                            average2 = average2_[0][0]
                        else:
                            average2 = 0
                        average_tot = (average + average2)/2
                        writer.writerow([holder_name, dataset, metric, principle, average_tot])

        with open('assets/prova3.csv', 'a', newline='') as file_csv:
            writer = csv.writer(file_csv)

            cursor.execute("""SELECT idHolder, nameHolder from Holder""")
            holders = [holder for holder in cursor.fetchall()]

            for holder, holder_name in holders:
                holder_average = 0
                dataset_number = 0
                cursor.execute("""SELECT idDataset from Dataset WHERE holderDataset = %s""", (holder,))
                holder_datasets = [dataset[0] for dataset in cursor.fetchall()]

                for dataset in holder_datasets:
                    dataset_average = 0
                    cursor.execute(
                        """SELECT idAssessment FROM Assessment WHERE idDataset = %s ORDER BY datetime DESC LIMIT 1""",
                        (dataset,))

                    last_assessment_ = cursor.fetchall()
                    if last_assessment_:
                        dataset_number += 1
                        last_assessment = last_assessment_[0][0]

                        for principle, metric in principi_metriche:
                            cursor.execute(f"""SELECT IFNULL(AVG({metric}), 0)
                            FROM DataAssessment d
                            WHERE idAssessment = %s""", (last_assessment,))

                            average_ = cursor.fetchall()
                            if average_:
                                average = average_[0][0]
                            else:
                                average = 0
                            cursor.execute(f"""SELECT IFNULL({metric}, 0) 
                            FROM MetadataAssessment 
                            WHERE idAssessment = %s""",
                                           (last_assessment,))
                            average2_ = cursor.fetchall()
                            if average2_:
                                average2 = average2_[0][0]
                            else:
                                average2 = 0
                            average_tot = (average + average2) / 2
                            dataset_average += average_tot

                        dataset_average = dataset_average / len(principi_metriche)

                        holder_average += dataset_average

                holder_average = holder_average / dataset_number

                writer.writerow([holder_name, holder_average, datetime.datetime.now()])
