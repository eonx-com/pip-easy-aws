from EasyLambda.Iterator.Filesystem.S3Filesystem import S3Filesystem
from EasyLambda.Iterator.Filesystem.SftpFilesystem import SftpFilesystem


class Filesystem:
    # Filesystem type constants
    FILESYSTEM_S3 = 'S3'
    FILESYSTEM_SFTP = 'SFTP'

    # Error constants
    ERROR_INVALID_FILESYSTEM = 'This filesystem has not been instantiated correctly, please use the relevant IteratorFilesystemFactory method to instantiate a file filesystem'

    def __init__(self, configuration):
        """
        This class should not be instantiated directly, please use the IteratorFilesystemFactory class

        :type configuration: dict
        :param configuration: The filesystems configuration
        """
        if configuration['filesystem'] == Filesystem.FILESYSTEM_S3:
            self.__filesystem__ = S3Filesystem(configuration=configuration)
        elif configuration['filesystem'] == Filesystem.FILESYSTEM_SFTP:
            self.__filesystem__ = SftpFilesystem(configuration=configuration)
        else:
            raise Exception(Filesystem.ERROR_INVALID_FILESYSTEM)

    def iterate_files(
            self,
            callback,
            filesystems,
            maximum_files=None,
    ):
        """
        Iterate files from the current source

        :type callback: function
        :param callback: User callback function that is executed for each iterated file

        :type filesystems: dict
        :param filesystems: The iterators filesystems

        :type maximum_files: int or None
        :param maximum_files: The maximum number of files to iterate

        :return: list List of files that were iterated over
        """
        return self.__filesystem__.iterate_files(
            callback=callback,
            filesystems=filesystems,
            maximum_files=maximum_files
        )

    def list_files(self):
        """
        List a list of all files accessible in the filesystem filesystem

        :return: list
        """
        # Ensure the filesystem is correctly configured
        if self.validate_configuration() is False:
            raise Exception(Filesystem.ERROR_INVALID_FILESYSTEM)
