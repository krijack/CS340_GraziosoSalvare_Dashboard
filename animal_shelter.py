
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId


class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, user, passwd, host, port, database, collection):
        # Use the parameters passed in (this was the source of your NameError)
        USER = user
        PASS = passwd
        HOST = host
        PORT = port
        DB   = database
        COL  = collection

        # Base URI; if your user was created in the same DB (aac), you might prefer authSource=DB
        uri = f"mongodb://{USER}:{PASS}@{HOST}:{PORT}"
        # uri = f"mongodb://{USER}:{PASS}@{HOST}:{PORT}/{DB}?authSource={DB}"

        # Initialize Connection
        try:
            self.client = MongoClient(uri, serverSelectionTimeoutMS=8000)
            self.client.admin.command("ping")           # fail fast if not reachable
            self.database = self.client[DB]
            self.collection = self.database[COL]
        except Exception as e:
            self.client = None
            self.database = None
            self.collection = None
            raise RuntimeError(f"Mongo connection failed: {e}")

    # Return next available record number
    def getNextRecordNum(self):
        out = self.collection.find().sort([("rec_num", -1)]).limit(1)
        docs = list(out)
        if docs:
            return int(docs[0].get("rec_num", 0)) + 1
        return 1

    # Complete this create method to implement the C in CRUD.
    def create(self, data):
        """
        Inserts one or more documents.
        If data is a dict, it will be wrapped in a list.
        Returns True on success; raises if input invalid.
        """
        if data is None:
            raise Exception("Nothing to save, because data parameter is empty")

        docs = data if isinstance(data, list) else [data]
        if not all(isinstance(d, dict) for d in docs):
            raise ValueError("create() expects a dict or list of dicts")

        try:
            for i in docs:
                # assign sequential rec_num
                i.pop("_id", None)  # avoid duplicate _id if cloning docs
                i.update({"rec_num": self.getNextRecordNum()})
                self.collection.insert_one(i)
            return True
        except PyMongoError as exc:
            print(f"ERROR during create: {exc}")
            return False

    # Create method to implement the R in CRUD.
    def read(self, data, projection=None):
        """
        Reads documents.
        :param data: filter dict; if None/{} returns first record (assignment-compatible)
        :param projection: optional dict (e.g., {'_id': 0})
        :return: list of documents
        """
        try:
            if data:
                cursor = self.collection.find(data, projection).sort([('rec_num', 1)])
                return list(cursor)
            else:
                doc = self.collection.find_one({}, projection)
                return [doc] if doc else []
        except PyMongoError as exc:
            print(f"ERROR during read: {exc}")
            return []

    # Method used to update a record in the database
    def update(self, query, data):
        """
        Updates documents matching 'query' with update doc (e.g., {'$set': {...}}).
        Returns number of documents modified.
        """
        if not query or not isinstance(data, dict):
            raise ValueError("update() requires a query dict and an update dict")
        try:
            ret = self.collection.update_many(query, data)
            return ret.modified_count
        except PyMongoError as exc:
            print(f"ERROR during update: {exc}")
            return 0

    # Method used to delete a record from the collection
    def delete(self, query):
        """
        Deletes documents matching 'query'.
        Returns number of documents deleted.
        """
        if not query:
            raise ValueError("delete() requires a non-empty query")
        try:
            ret = self.collection.delete_many(query)
            return ret.deleted_count
        except PyMongoError as exc:
            print(f"ERROR during delete: {exc}")
            return 0

    # Method used to test connection to database
    def con_test(self):
        return self.collection.find_one()


# Optional manual test harness
def main():
    print("Main Method")
    print("Setting up Database Connection")
    db = AnimalShelter('aacuser', 'SNHU1234', '127.0.0.1', 27017, 'aac', 'animals')

    print("Connection Test")
    print(db.con_test())

    print("Read Test - no param")
    print(db.read(None))

    print("Read Test - breed")
    rec = db.read({ "breed" : "Labrador Retriever Mix" })
    print(len(rec))
    for i in range(min(5, len(rec))):
        print(rec[i])

    print("Record Number Test")
    print(db.getNextRecordNum())

    print("Testing in create: (main)")
    if rec:
        dummy = rec[0].copy()
        dummy.pop("_id", None)
        result = db.create(dummy)
        print("Create Result:", result)

        print("Testing Update:")
        retVal = db.update({ "animal_id": dummy.get("animal_id", "B721406") },
                           { "$set": { "note": "updated in main()" }})
        print(f"{retVal} records updated.")

        print("Testing removal of last document added...")
        lastVal = db.getNextRecordNum() - 1
        retVal = db.delete({ "rec_num" : lastVal })
        print(f"{retVal} records deleted from the collection.")

if __name__ == "__main__":
    main()
