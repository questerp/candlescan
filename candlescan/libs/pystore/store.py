

import os
import shutil

from . import utils
from .collection import Collection


class store(object):
    def __repr__(self):
        return "PyStore.datastore <%s>" % self.datastore

    def __init__(self, datastore ):

        datastore_path = utils.get_path()
        if not utils.path_exists(datastore_path):
            os.makedirs(datastore_path)

        self.datastore = utils.make_path(datastore_path, datastore)
        if not utils.path_exists(self.datastore):
            os.makedirs(self.datastore)
       
        self.collections = self.list_collections()

    def _create_collection(self, collection, overwrite=False):
        # create collection (subdir)
        collection_path = utils.make_path(self.datastore, collection)
        if utils.path_exists(collection_path):
            if overwrite:
                self.delete_collection(collection)
            else:
                raise ValueError(
                    "Collection exists! To overwrite, use `overwrite=True`")

        os.makedirs(collection_path)
        os.makedirs(utils.make_path(collection_path, "_snapshots"))

        # update collections
        self.collections = self.list_collections()

        # return the collection
        return Collection(collection, self.datastore)

    def delete_collection(self, collection):
        # delete collection (subdir)
        shutil.rmtree(utils.make_path(self.datastore, collection))

        # update collections
        self.collections = self.list_collections()
        return True

    def list_collections(self):
        # lists collections (subdirs)
        return utils.subdirs(self.datastore)

    def collection(self, collection, overwrite=False):
        if collection in self.collections and not overwrite:
            return Collection(collection, self.datastore )

        # create it
        self._create_collection(collection, overwrite)
        return Collection(collection, self.datastore )

    def item(self, collection, item):
        # bypasses collection
        return self.collection(collection).item(item)