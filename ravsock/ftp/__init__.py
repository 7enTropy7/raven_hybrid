from ftplib import FTP

from ..config import FTP_SERVER_URL


class FTPClient:
    def __init__(self, host, user, passwd):
        self.ftp = FTP(host=host)
        self.ftp.set_pasv(True)
        self.ftp.login(user, passwd)

    def download(self, filename, path):
        print("Downloading")
        self.ftp.retrbinary("RETR " + path, open(filename, "wb").write)
        print("Downloaded")

    def upload(self, filename, path):
        print("Uploading")
        self.ftp.storbinary("STOR " + path, open(filename, "rb"))
        print("Uploaded")

    def list_server_files(self):
        self.ftp.retrlines("LIST")

    def close(self):
        self.ftp.quit()


def get_client(host=None, username=None, password=None):
    print("Credentials", host, username, password)
    if host is None:
        host = FTP_SERVER_URL
    print("FTP User credentials:", host, username, password)
    return FTPClient(host=host, user=username, passwd=password)


def check_credentials(host=None, username=None, password=None):
    if host is None:
        host = FTP_SERVER_URL
    print("Credentials", host, username, password)
    try:
        FTPClient(host=host, user=username, passwd=password)
        return True
    except Exception as e:
        print("Error:{}".format(str(e)))
        return False
