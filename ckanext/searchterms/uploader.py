import datetime
import os

from ckan.common import config
import ckan.lib.uploader as uploader

"""
Implements requirements for a resource uploader, but saves to local filesystem
instead of uploading
"""


class ResourceUpload(object):
    def __init__(self, resource):
        path = uploader.get_storage_path()

        if not path:
            self.storage_path = None
            return
        self.storage_path = os.path.join(path, "resources")
        try:
            os.makedirs(self.storage_path)
        except OSError as e:
            # errno 17 is file already exists
            if e.errno != 17:
                raise
        self.filename = None
        self.mimetype = "text/tab-separated-values"

        if resource.get("upload", None) != None:
            upload_obj = resource.pop("upload", None)
            self.df = upload_obj.get("df")  # pandas dataframe
            self.filename = upload_obj.get("filename")
            resource["url"] = "{}.tsv".format(self.filename)

        resource["url_type"] = "upload"
        resource["last_modified"] = datetime.datetime.utcnow()

    def get_directory(self, id):
        directory = os.path.join(self.storage_path, id[0:3], id[3:6])
        return directory

    def get_path(self, id):
        """Required by the resource_download action to determine the path to the file."""
        directory = self.get_directory(id)
        filepath = os.path.join(directory, id[6:])
        return filepath

    def upload(self, id, max=None):
        """Save the file.

        :returns: ``'file uploaded'`` if a new file was successfully uploaded
            (whether it overwrote a previously uploaded file or not),
            ``'file deleted'`` if an existing uploaded file was deleted,
            or ``None`` if nothing changed
        :rtype: ``string`` or ``None``

        """
        if not self.storage_path:
            return

        # Get directory and filepath on the system
        # where the file for this resource will be stored
        directory = self.get_directory(id)
        filepath = self.get_path(id)

        # If a filename has been provided (a file is being uploaded)
        # we write it to the filepath (and overwrite it if it already
        # exists). This way the uploaded file will always be stored
        # in the same location
        if self.filename:
            try:
                os.makedirs(directory)
            except OSError as e:
                # errno 17 is file already exists
                if e.errno != 17:
                    raise
            self.df.to_csv(filepath, sep="\t", index=False, na_rep="")
            return
