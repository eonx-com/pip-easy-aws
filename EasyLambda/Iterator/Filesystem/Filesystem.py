from EasyLambda.Iterator.Filesystem.File import File
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

    def stake(self, remote_filename) -> bool:
        """
        Attempt to claim exclusive ownership of a file in the remote filesystem

        :type remote_filename:
        :param remote_filename:

        :return: bool
        """
        return self.__filesystem__.stake(
            remote_filename=remote_filename
        )

    def exists(self, remote_filename) -> bool:
        """
        Check if the specified remote filename exists

        :type remote_filename: bool
        :param remote_filename: Remote path/filename to check

        :return: bool
        """
        return self.__filesystem__.exists(
            remote_filename=remote_filename
        )

    def list(self, path, recursive=False):
        """
        List a list of all files accessible in the filesystem filesystem

        :type path: str
        :param path: The path in the filesystem to list

        :type recursive: bool
        :param recursive: If True the listing will proceed recursively down through all sub-folders

        :return: list
        """
        return self.__filesystem__.list(
            path=path,
            recursive=recursive
        )

    def delete(self, remote_filename) -> None:
        """
        Delete file from filesystem

        :type remote_filename: str
        :param remote_filename: Path/filename of remote file to be deleted

        :return: None
        """
        return self.__filesystem__.delete(
            remote_filename=remote_filename
        )

    def upload(self, local_filename, remote_filename) -> File:
        """
        Upload file from local filesystem to destination

        :type local_filename: str
        :param local_filename: Path/filename of local file to be uploaded

        :type remote_filename: str
        :param remote_filename: Destination path/filename in remote filesystem

        :return: File
        """
        return self.__filesystem__.upload(
            local_filename=local_filename,
            remote_filename=remote_filename
        )

    def download(self, local_filename, remote_filename) -> File:
        """
        Download file from remote filesystem to local filesystem

        :type remote_filename: str
        :param remote_filename: Destination path/filename in local filesystem

        :type local_filename: str
        :param local_filename: Path/filename of local file to be uploaded

        :return: File
        """
        return self.__filesystem__.download(
            local_filename=local_filename,
            remote_filename=remote_filename
        )
