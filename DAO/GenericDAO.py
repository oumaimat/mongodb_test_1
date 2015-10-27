__author__ = 'OTurki'

import pymongo


class ConnectionToDatabase :

    db_conn=None
    database = None

    #penser a creer un pool de connections reutilisables vers la base
    #solution actuelle non envisageable en prod

    def __init__(self):
        self.connectToDatabase()


    def connectToDatabase(self) :
        # Connexion au serveur de Mongo DB

        try:
            # db_conn=pymongo.MongoClient("mongodb://admin:3usRy1SmJ8gH@127.0.0.1:37391/")
            db_conn=pymongo.MongoClient()
            print("Connected successfully!!!")
            print(db_conn)
        except pymongo.errors.ConnectionFailure :
            print("Could not connect to MongoDB: %s")


        # Connexion a la base du projet
        #db = db_conn["mongodbtest0"]
        db = db_conn["GPSLocationDB"]


        #Initialisation des variables globales
        ConnectionToDatabase.database = db


    def getCollection(self, collection_name):

        #Connexion a la collection
        collection = ConnectionToDatabase.database[collection_name]

        #function result
        return collection

class GenericDAO :

    # Mettre a jour un enregistrement dans une collection et inserer si pas d'enregistrement trouve
    def updateObjects(self, collection, objectCriteria, objectUpdate):

        updateResult = collection.update(objectCriteria, objectUpdate, upsert = True)

        return updateResult


    # Inserer un enregistrement dans une collection
    def insertObject(self, collection, object):

        insertionResult = collection.insert(object)

        return insertionResult
