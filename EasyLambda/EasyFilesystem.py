from EasyLambda.EasyLog import EasyLog


class EasyFilesystem:
    def __init__(self, driver):
        """
        Setup filesystem

        :type driver: EasyFilesystemDriverS3 or EasyFilesystemDriverSftp
        :param driver: The underlying filesystem driver
        """
        EasyLog.trace('Instantiating filesystem...')

        # Store the underlying driver
        self.__driver__ = driver

    def file_list(self, filesystem_path, recursive=False) -> list:
        """
        List files in the filesystem path

        :type filesystem_path: str
        :param filesystem_path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list
        """
        return self.__driver__.file_list(
            path=filesystem_path,
            recursive=recursive
        )

    def file_exists(self, filesystem_filename) -> bool:
        """
        Check if file exists in the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to check in the filesystem (relative to whatever base path may be defined)

        :return: bool
        """
        return self.__driver__.file_exists(
            filesystem_filename=filesystem_filename
        )

    def file_delete(self, filesystem_filename) -> None:
        """
        Delete a file from the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to be deleted from the filesystem (relative to whatever base path may be defined)

        :return: None
        """
        self.__driver__.file_delete(
            filesystem_filename=filesystem_filename
        )

    def file_download(self, filesystem_filename, local_filename):
        """
        Download a file from the filesystem to local storage

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file in the filesystem to download (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The destination on the local filesystem where the file will be downloaded to

        :return:
        """
        self.__driver__.file_download(
            filesystem_filename=filesystem_filename,
            local_filename=local_filename
        )

    def file_upload(self, filesystem_filename, local_filename):
        """
        Upload a file from local storage to the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The destination on the filesystem where the file will be uploaded to  (relative to whatever base path may be defined)

        :type local_filename: str
        :param local_filename: The name of the local filesystem file that will be uploaded

        :return: None
        """
        self.__driver__.file_upload(
            filesystem_filename=filesystem_filename,
            local_filename=local_filename
        )
