import os


class SftpFile:
    def __init__(self, filesystem, filename):
        """
        :type filesystem: SftpFilesystem
        :param filesystem: The filesystem in which the file is located

        :type filename: str
        :param filename: Path/filename on SFTP server
        """
        self.__filesystem__ = filesystem
        self.__filename__ = filename

    def get_filename(self) -> str:
        """
        Get the filename

        :return: str
        """
        return self.__filename__

    def get_basename(self) -> str:
        """
        Get the filename only

        :return: str
        """
        return os.path.basename(self.__filename__)

    def get_path(self) -> str:
        """
        Get the name of the path where the file is located

        :return: str
        """
        return os.path.dirname(self.__filename__)

    def download(self, local_filename):
        """
        Download the file

        :type local_filename: str
        :param local_filename: The destination on the local filesystem

        :return: File
        """
        return self.__filesystem__.download(
            remote_filename=self.__filename__,
            local_filename=local_filename
        )

    def upload(self, local_filename) -> None:
        """
        Upload file from local disk

        :type local_filename: str
        :param local_filename: The local file to upload

        :return: None
        """
        self.__filesystem__.upload(
            remote_filename=self.__filename__,
            local_filename=local_filename
        )