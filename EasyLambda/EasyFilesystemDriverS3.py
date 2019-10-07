from EasyLambda.EasyFilesystemDriver import EasyFilesystemDriver
from EasyLambda.EasyLog import EasyLog
from EasyLambda.EasyS3Bucket import EasyS3Bucket


class EasyFilesystemDriverS3(EasyFilesystemDriver):
    def __init__(
            self,
            bucket_name,
            base_path
    ):
        """
        Instantiate S3 filesystem driver

        :type bucket_name: str
        :param bucket_name: The S3 bucket name

        :type base_path: str
        :param base_path: The base path in the S3 bucket
        """
        EasyLog.trace('Instantiating S3 filesystem driver...')

        self.__s3_bucket__ = EasyS3Bucket(
            bucket_name=bucket_name,
            base_path=base_path
        )

    def file_list(self, filesystem_path, recursive=False) -> list:
        """
        List files in the filesystem path

        :type filesystem_path: str
        :param filesystem_path: Path in the filesystem to list (relative to whatever base path may be defined)

        :type recursive: bool
        :param recursive: Flag indicating the listing should be recursive. If False, sub-folder contents will not be returned

        :return: list
        """
        return self.__s3_bucket__.file_list(
            bucket_path=filesystem_path,
            recursive=recursive
        )

    def file_exists(self, filesystem_filename) -> bool:
        """
        Check if file exists in the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to check in the filesystem (relative to whatever base path may be defined)

        :return: bool
        """
        return self.__s3_bucket__.file_exists(
            bucket_filename=filesystem_filename
        )

    def file_delete(self, filesystem_filename) -> None:
        """
        Delete a file from the filesystem

        :type filesystem_filename: str
        :param filesystem_filename: The name of the file to be deleted from the filesystem (relative to whatever base path may be defined)

        :return: None
        """
        self.__s3_bucket__.file_delete(
            bucket_filename=filesystem_filename
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
        self.__s3_bucket__.file_download(
            bucket_filename=filesystem_filename,
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
        self.__s3_bucket__.file_upload(
            bucket_filename=filesystem_filename,
            local_filename=local_filename
        )
