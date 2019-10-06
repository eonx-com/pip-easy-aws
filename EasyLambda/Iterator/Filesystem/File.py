class File:
    def __init__(self):
        """
        This class should not be instantiated directly, instances should be created by using the relevant filesystems class methods
        """
        self.filesystem = None
        self.configuration = {}

    def copy(
            self,
            destination,
            destination_filename,
            allow_overwrite=False
    ):
        """
        Copy the file to a new destination

        :type destination: Destination
        :param destination: The destination to which the file should be copied

        :type destination_filename: str
        :param destination_filename: The path/filename on the destination filesystem where this should be copied

        :type allow_overwrite: bool
        :param allow_overwrite: If False an exception will be raised if the copy would overwrite an existing file

        :return: File The corresponding file object in the destination filesystem
        """
        pass

    def move(
            self,
            destination,
            destination_filename,
            allow_overwrite=False
    ):
        """
        Move the file to a new destination

        :type destination: Destination
        :param destination: The destination to which the file should be copied

        :type destination_filename: str
        :param destination_filename: The path/filename on the destination filesystem where this should be copied

        :type allow_overwrite: bool
        :param allow_overwrite: If False an exception will be raised if the copy would overwrite an existing file

        :return: File The corresponding file object in the destination filesystem
        """
        pass

    def download(self, destination_filename) -> int:
        """
        Download the file to the local filesystem

        :type destination_filename: str
        :param destination_filename: The path/filename on the local filesystem where this

        :return: int The size of the downloaded file
        """
        pass

    def replace(self, file):
        """
        Replace the content of the file with the content of the other file. The file passed to the function will not be affected by this action

        :type file: File
        :param file: The file which should overwrite the existing files content

        :return: None
        """
        pass

    def delete(self) -> bool:
        """
        Delete the file from its filesystem

        :return: bool
        """
        pass
