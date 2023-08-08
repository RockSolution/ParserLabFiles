MSSQL_SERVER = "localhost"
MSSQL_USER = "sa"
MSSQL_PASSWORD = ""
MSSQL_DB = "SDRS"
SEPARATOR = ","
TABLE_COMPARE = {
    "BonelessBeef": "BonelessBeef_MicroTest",
    "Fat": "FinishedProd_FatTest",
    "FinishedProduct": "FinishedProd_MicroTest"
}
FALLEN_FILES_FOLDER = "ProcessedFiles"
PROCESSED_FILES_FOLDER = "ProcessedFiles"

FTP_IP = ''
FTP_USER = ''
FTP_PASSWORD = ''
# if True then delete downloaded files at ftp server
DELETE_FTP_FILES = False
# if you need parse local files in folders and don't need download files from ftp
DOWNLOAD_FROM_FTP = True
