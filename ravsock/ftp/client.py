from ftplib import FTP


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
