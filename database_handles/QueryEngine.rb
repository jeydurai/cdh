module QueryEngine

  class MySQLQuery
    attr_accessor :sth
    
    def getCursor(conn, queryString)
      @sth = conn.query(queryString)
      return @sth
    end
    
    def getCursorByParameters(conn, queryString, paramHash)
      @sth = conn.prepare(queryString)
      keyCount = paramHash.count()
      case keyCount
      when 0
        @sth.execute()
      when 1
        @sth.execute(paramHash['param1'])
      when 2
        @sth.execute(paramHash['param1'], paramHash['param2'])
      when 3
          @sth.execute(paramHash['param1'], paramHash['param2'], paramHash['param3'])
      when 42
          @sth.execute(paramHash['param1'], paramHash['param2'], paramHash['param3'], \
                       paramHash['param4'], paramHash['param5'], paramHash['param6'], \
                       paramHash['param7'], paramHash['param8'], paramHash['param9'], \
                       paramHash['param10'], paramHash['param11'], paramHash['param12'], \
                       paramHash['param13'], paramHash['param14'], paramHash['param15'], \
                       paramHash['param16'], paramHash['param17'], paramHash['param18'], \
                       paramHash['param19'], paramHash['param20'], paramHash['param21'], \
                       paramHash['param22'], paramHash['param23'], paramHash['param24'], \
                       paramHash['param25'], paramHash['param26'], paramHash['param27'], \
                       paramHash['param28'], paramHash['param29'], paramHash['param30'], \
                       paramHash['param31'], paramHash['param32'], paramHash['param33'], \
                       paramHash['param34'], paramHash['param35'], paramHash['param36'], \
                       paramHash['param37'], paramHash['param38'], paramHash['param39'], \
                       paramHash['param40'], paramHash['param41'], paramHash['param42'], \
          )
      end
      
      return @sth 
    end
    
    public :getCursor, :getCursorByParameters
  end
  
  class MongoQuery
    attr_accessor :cursor, :status, :coll

    def initialize(client, collectionName)
      @coll = client[collectionName]
    end
    
    # Method to fetch Collection
    def getCollection
      return @coll
    end
    
    # Method to insert a single document in any collection
    def insertDocument(collection, document)
      return collection.insert_one(document)
    end
    
    # Method to remove any collection
    def removeCollection(collection)
      collection.drop
    end

    # Method to check whether any document exists for a query
    def checkIfExists(collection, query)
      cursorString = collection.find(query).to_a
      if cursorString.any?
        status = 1
      else
        status = 0
      end
      return status
    end
    
    # Method to return Documents using a query
    def findAndFetchDocuments(collection, query)
        return collection.find(query)
    end

    # Method that runs a aggregate query and return documents
    def aggregateAndFetchDocuments(collection, aggregateArray)
        return collection.aggregate(aggregateArray)
    end

    # Method that runs a aggregate query and write documents
    def aggregateAndWriteDocuments(collection, aggregateArray)
        collection.aggregate(aggregateArray)
    end
    
    # Method to return a Document by username
    def fetchUserByID(collection, userName)
        query = Hash.new()
        query = {
           :username => userName,
          "approval_status.code" => 1 
        }
        return collection.find(query)
    end
    
    # Method to map/update username in booking_dump collection
    # based on truenorth user list in admin module
    def mapUsernameInBookingDump(collection, query, userName)
        collection.update_many(
            query,
            {
                :$addToSet => {
                    :mappedTo => userName
                }
            }
        )
    end

    # Method to unmap/remove username from booking_dump collection
    # to make sure that the particular user can not have view
    def unMapUsernameInBookingDump(collection, userName)
        collection.update_many(
            {
                :$pull => {
                    :mappedTo => userName
                }
            }
        )
    end

    # Initialization of username mapping that removes the field
    # 'mappedTo' -- Care needs to be taken when using it
    def removeMappedToField(collection)
        collection.update_many(
            {},
            {
                :$unset => {
                    :mappedTo => ""
                }
            }
        ) 
    end

    # Change the user approval status to 1 (Activate an User)
    def activateUser(collection, userName)
        collection.update_many(
            {:username => userName},
            {
                :$set => {
                    "approval_status.code" => 1, 
                    "approval_status.description" => "ACTIVATED",
                }
            }
        ) 
    end

    # Change the user approval status to 2 (back to Approved Status)
    def setAnUserApproved(collection, userName)
        collection.update_many(
            {:username => userName},
            {
                :$set => {
                    "approval_status.code" => 2, 
                    "approval_status.description" => "APPROVED",
                }
            }
        ) 
    end

    # Change the user approval status to 2 (back to Approved status)
    def setUsersApproved(collection)
        collection.update_many(
            {
                "approval_status.code" => 1, 
            },
            {
                :$set => {
                    "approval_status.code" => 2, 
                    "approval_status.description" => "APPROVED",
                }
            }
        ) 
    end

    # Method to return documents of all rejected and purged users
    def fetchAllUsersByRejectedPurged(collection)
        query = Hash.new()
        query = {
            :$or => [
                {
                    "approval_status.code" => -1
                },
                {
                    "approval_status.code" => -2
                }
            ]
        } 
        return collection.find(query)
    end 

    # Method to return documents of all approved users
    def fetchAllUsersByApproved(collection)
        query = Hash.new()
        query = {
            "approval_status.code" => 2
        } 
        return collection.find(query)
    end 
# Simple MongoDB query to fetch all the documents from any
    # collection -- Care needs to be taken as the memory may be
    # overloaded if there exist more documents
    def fetchAllDocuments(collection)
        return collection.find({})
    end 

    # Method update one matching documents for a query
    def updateSubDocument(collection, query, document)
      collection.find(query).update_one(document)
    end
    
    # Method to create one single Index
    def createIndex(collection, indexObject)
      collection.indexes.create_one(indexObject)
    end

    # Method to remove a Index
    def removeIndex(collection, indexObject)
      collection.indexes.drop_one(indexObject)
    end

    # Method to remove All Indexes
    def removeIndexes(collection)
      collection.indexes.drop_all()
    end
    
    public :getCollection, :insertDocument, :checkIfExists, :updateSubDocument, \
        :fetchAllDocuments,:fetchAllUsersByApproved, :fetchAllUsersByRejectedPurged, \
        :removeMappedToField, :unMapUsernameInBookingDump, :mapUsernameInBookingDump, \
        :fetchUserByID, :aggregateAndFetchDocuments, :findAndFetchDocuments, \
        :removeCollection
  end
  
end
