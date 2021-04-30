import os
import shutil
import typing
from functools import lru_cache

import requests
from google.cloud import storage
from google.oauth2 import service_account


# TODO: this should probably be replaced with something
# more general, and the ModelRepo can be handled as a
# file system backend in https://github.com/alecgunny/exportlib
class GCSModelRepo:
    def __init__(
        self,
        bucket_name: str,
        credentials: typing.Optional[service_account.Credentials] = None,
    ):
        self.bucket_name = bucket_name
        self._client = storage.client.Client(credentials=credentials)

    @property
    @lru_cache(None)
    def bucket(self):
        try:
            return self._client.get_bucket(self.bucket_name)
        except requests.HTTPError as e:
            if e.code == 404:
                try:
                    return self._client.create_bucket(self.bucket_name)
                except Exception:
                    # TODO: some check on whether this already exists
                    raise
            raise

    def export_repo(
        self, repo_dir: str, start_fresh: bool = True, clear: bool = True
    ):
        if start_fresh:
            for blob in self._client.list_blobs(self.bucket):
                blob.delete()

        for root, _, files in os.walk(repo_dir):
            for f in files:
                path = os.path.join(root, f)

                # get rid of root level path and replace
                # path separaters in case we're on Windows
                blob_path = path.replace(os.path.join(repo_dir, ""), "").replace(
                    "\\", "/"
                )
                print(f"Copying {path} to {blob_path}")

                blob = self.bucket.blob(blob_path)
                blob.upload_from_filename(path)

        if clear:
            for d in next(os.walk(repo_dir))[1]:
                print(f"Removing model {d}")
                shutil.rmtree(os.path.join(repo_dir, d))
