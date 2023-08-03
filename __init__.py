import os
import pymssql
import pandas as pd
import config
import glob
import shutil
import logging
from zipfile import ZipFile
from datetime import datetime


class ParseLabFiles:

    def __init__(self) -> None:
        self.conn = object
        self.cursor = object
        self.init_logger('errors')
        self.init_logger('success')
        self.log_error = logging.getLogger('errors')
        self.log_success = logging.getLogger('success')

        """
            dict of comparison for change wrong columns from csv on correct columns in DB 
            key - is column in CSV, value - column of table
        """
        self.columns_compr = {
            'Material_Description': 'Material_Code',
            'E_coli_O157_H7': 'E_Coli_0157_H7',
            'E_Coli_O157_H7_Cl': 'E_Coli_0157_H7_Cl',
            'E_Coli_O157_H7': 'E_Coli_0157_H7'
        }

    def run(self) -> None:
        self.log_success.info('Parser started')
        self.connect()
        paths = self.get_lab_path()
        self.parse_files_to_df(paths)
        self.conn.close()
        self.log_success.info('Parser completed')

    # setup logger
    def init_logger(self, logger_file: str) -> None:
        file_name = logger_file + '.log'
        logger = logging.getLogger(logger_file)
        logger.setLevel(logging.ERROR if logger_file == 'errors' else logging.INFO)
        fh = logging.FileHandler(file_name)
        formatter = logging.Formatter('%(asctime)s[%(levelname)s]: %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    # connect to DB
    def connect(self) -> None:
        try:
            self.conn = pymssql.connect(config.MSSQL_SERVER, config.MSSQL_USER, config.MSSQL_PASSWORD, config.MSSQL_DB)
            self.cursor = self.conn.cursor(as_dict=True)
        except Exception as e:
            self.log_error.error(e)

    # gets folder paths
    def get_lab_path(self) -> list:
        paths = []
        # select Labs that is active in DB
        self.cursor.execute('SELECT * FROM Labs WHERE active=%s', '1')
        labs = self.cursor.fetchall()
        # if no active Labs show error
        if len(labs) > 0:
            paths = [row['Lab_Name'] for row in labs]
        else:
            self.conn.close()
            try:
                raise Exception("No Active Laboratories in DB")
            except Exception as e:
                self.log_error.error(e)
                print('Error: ' + repr(e))

        return paths

    # parse files in lab folders
    def parse_files_to_df(self, paths: list) -> None:
        for path in paths:
            # gets files in folder
            files = [item for sublist in [glob.glob(path + '/' + ext) for ext in ['*.csv']] for item in sublist]
            if len(files) > 0:
                # file by file
                for file in files:
                    file_name, file_extension = os.path.splitext(file)
                    table_name = file_name = file_name.split('/')[-1]
                    # lab name is name of folder
                    lab_name = path
                    try:
                        # read CSV files
                        df = pd.read_csv(file, sep=config.SEPARATOR, keep_default_na=False)
                    except Exception as e:
                        self.process_file(False, lab_name, file_name + file_extension, e)
                        pass
                    else:
                        df = pd.read_csv(file, sep=config.SEPARATOR, keep_default_na=False)
                        # replace all empty with None
                        df = df.replace('', None)
                        # make all values as string
                        df = df.applymap(str)
                        # some columns contains - replace it to _, otherwise will be DB error
                        df.columns = df.columns.str.replace('-', '_')
                        # change columns on correct
                        df.rename(columns=self.columns_compr, inplace=True)
                        # insert data to db
                        self.insert(lab_name, table_name, file_name + file_extension, df)
            else:
                msg = f'In folder {path} no files fo parse'
                print(msg)
                self.log_success.info(msg)

    def column_replace(self, x: str) -> str:
        if x in self.columns_compr:
            return self.columns_compr[x]
        else:
            return x

    # time in MSSQL format
    def get_time(self) -> str:
        t = datetime.now()
        s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
        return str(s[:-3])

    # insert data to DB
    def insert(self, lab_name: str, table_name: str, file_name: str, df: pd.DataFrame) -> None:
        # total rows
        total_rows = len(df)

        if total_rows > 0:
            # insert info about uploaded file
            insert_query = f"INSERT INTO File_Uploaded(Filename, UploadTimeStamp, LabName, TotalRecords) VALUES (%s, %s, %s, %s)"
            sql_data = [(file_name, self.get_time(), lab_name, total_rows)]
            self.cursor.executemany(insert_query, sql_data)
            # get ID inserted file
            file_id = self.cursor.lastrowid
            # set in dataframe data about lab_name and file_id
            df['Lab_Name'] = str(lab_name)
            df['FileID'] = str(file_id)

            # gets columns from header of csv file
            columns = ", ".join(df.columns)
            # make string with count of columns where replace name to %s for insert query
            value_fields = ", ".join(['%s' for _ in df.columns])

            # insert data from csv
            insert_query = f"INSERT INTO {config.TABLE_COMPARE[table_name]}({columns}) VALUES ({value_fields})"
            sql_data = list(map(lambda x: tuple(map(lambda y: None if y == '' else y, x)), df.values))

            try:
                self.cursor.executemany(insert_query, sql_data)
                self.conn.commit()
            except Exception as e:
                self.process_file(False, lab_name, file_name, e)
                pass
            else:
                self.process_file(True, lab_name, file_name)
                msg = f'Completed! Total rows {total_rows} inserted to table ' \
                    f'{table_name} from file {file_name}, Lab Name: {lab_name}.'
                print(msg)

    # move files to folders in case of success processing or with errors
    def process_file(self, file_ok: bool, lab_name: str, file_name: str, error=None) -> None:
        file_time = str(datetime.now())

        if file_ok:
            if not os.path.isdir('ProcessedFiles/' + lab_name):
                os.mkdir('ProcessedFiles/' + lab_name)

            zip_file_path = 'ProcessedFiles/' + lab_name + '/' + file_time + '_' + file_name + ".zip"
            with ZipFile(zip_file_path, 'w') as zip:
                zip.write(lab_name + '/' + file_name)

            msg = f"The File {file_name} from Lab {lab_name} processed successfully and moved to {zip_file_path}"
            print(msg)
            self.log_success.info(msg)
        else:
            if not os.path.isdir('FallenFiles/' + lab_name):
                os.mkdir('FallenFiles/' + lab_name)
            fallen_file_path = 'FallenFiles/' + lab_name + '/' + file_time + '_' + file_name
            shutil.copy(lab_name + '/' + file_name, fallen_file_path)

            msg = f"The File {file_name} from Lab {lab_name} has errors and moved to {fallen_file_path}. {error}"
            print(msg)
            self.log_error.error(msg)
        os.remove(lab_name + '/' + file_name)


if __name__ == "__main__":
    app = ParseLabFiles()
    app.run()
