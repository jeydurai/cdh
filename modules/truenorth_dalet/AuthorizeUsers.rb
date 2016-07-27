module AuthorizeUsers

    # Namespace, Module and Class Loading
    require './database_handles/Connections'
    require './database_handles/QueryEngine'
    include Connections
    include QueryEngine


    class DeactivateUsers
        attr_accessor :queryObj, :mongoObj
        
        def initialize()
            puts "Acquiring MongoDB Object....\n"
            @mongoObj = Connections::MongoDBClient.new()
            puts "MongoDB Object acquired!\n"
        end

        # One time initialization to get rid of mappedTo field that was created
        # in testing phase
        def refreshMappedToField()
            puts "Removing mappedTo field from booking_dump collection\n"
            mongoClient = @mongoObj.getMongoClient()
            mongoEngine2 = QueryEngine::MongoQuery.new(mongoClient, 'booking_dump2')
            mongoEngine2.removeMappedToField(mongoEngine2.getCollection())
            puts "mappedTo field has been successfully removed from booking_dump collection\n"
        end


        # Method to unauthorize a user
        def deactivateUsers()
            # Preparing MongoDB Connection Objects
            mongoClient = @mongoObj.getMongoClient()
            mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'users')
            docs = mongoEngine.setUsersApproved(mongoEngine.getCollection())
            puts "Done!\n"
        end

        # Method to unauthorize a user
        def unAuthorizeRejectedUser(userName="")
            # Preparing MongoDB Connection Objects
            puts "Removing #{userName} from booking_dump collection\n"
            mongoClient = @mongoObj.getMongoClient()
            mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'users')
            mongoEngine2 = QueryEngine::MongoQuery.new(mongoClient, 'booking_dump')
            userName = 'timitra'
            docs = mongoEngine.fetchUserByID(mongoEngine.getCollection(), userName)
            puts "#{userName} has been removed from booking_dump collection\n"
        end
    end

    class ActivateUsers
        attr_accessor :queryObj, :mongoObj

        def initialize()
            puts "Acquiring MongoDB object....\n"
            @mongoObj = Connections::MongoDBClient.new()
            puts "MongoDB Object acquired!\n"
        end # End of Method Initialize

        # Method to Authorise a specific approved user
        def authorizeApprovedUser(userName)
            query = Hash.new()   
            # Preparing MongoDB Connection Objects
            mongoClient = @mongoObj.getMongoClient()
            mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'users')
            mongoEngine2 = QueryEngine::MongoQuery.new(mongoClient, 'booking_dump2')
            mongoEngine3 = QueryEngine::MongoQuery.new(mongoClient, 'unique_nodes_all')
            docs = mongoEngine.fetchUserByID(mongoEngine.getCollection(), userName)
            #docs = mongoEngine.fetchAllUsersByApproved(mongoEngine.getCollection())
            docs.each do |doc|
                # Acquiring all the required nodes in local variables
                userName = doc[:username]
                sl6Array = doc[:accessibility][:location][:sales_level_6]
                salesAgents = doc[:accessibility][:location][:sales_agents]
                mongoEngine.activateUser(mongoEngine.getCollection(), userName)
                puts "#{userName}\n"
                # Loops to iterate through all possible combinations of nodes
                    salesAgents.each do |salesAgent|
                        matchHash = Hash.new()
                        groupHash = Hash.new()
                        aggregateArray= Array.new()
                        matchHash = {
                            :$match => {
                                "sales_agents" => salesAgent
                            }
                        }
                        groupHash = {
                            :$group => {
                                :_id => "$sales_level_6",
                                :recs => {
                                    :$sum => 1
                                }
                            }
                        }
                        aggregateArray= [
                            matchHash, groupHash
                        ]
                        aggreDocs = mongoEngine3.aggregateAndFetchDocuments(mongoEngine3.getCollection(), aggregateArray)
                        matchedSL6 = Array.new()
                        arrayCounter = 0 
                        tempCounter = 0
                        aggreDocs.each do |doc2|
                            if sl6Array.include? doc2[:_id]
                                matchedSL6[arrayCounter] = doc2[:_id]
                                arrayCounter += 1
                                updateQuery = Hash.new()
                                updateQuery = {
                                    "names.sales_agent.name" => salesAgent,
                                    "location_nodes.sales_level_6" => doc2[:_id]
                                }
                                puts "Updating the Booking Data with mapping user name\n"
                                mongoEngine2.mapUsernameInBookingDump(mongoEngine2.getCollection(), updateQuery, userName)
                                puts "Booking Data is mapped with user name\n"
                            end
                            tempCounter += 1
                        end
                        puts "#{userName} - #{salesAgent} - #{matchedSL6} - #{arrayCounter}/#{tempCounter} rec(s)\n"
                        
                    end
            end # End of Enumeration each on docs
        end # End of Method authorizeAllApprovedUser

        # Method to Authorise all approved users
        def authorizeAllApprovedUsers()
            query = Hash.new()   
            # Preparing MongoDB Connection Objects
            mongoClient = @mongoObj.getMongoClient()
            mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'users')
            mongoEngine2 = QueryEngine::MongoQuery.new(mongoClient, 'booking_dump2')
            mongoEngine3 = QueryEngine::MongoQuery.new(mongoClient, 'unique_nodes_all')
            #docs = mongoEngine.fetchUserByID(mongoEngine.getCollection(), 'timitra')
            docs = mongoEngine.fetchAllUsersByApproved(mongoEngine.getCollection())
            docs.each do |doc|
                # Acquiring all the required nodes in local variables
                userName = doc[:username]
                sl6Array = doc[:accessibility][:location][:sales_level_6]
                salesAgents = doc[:accessibility][:location][:sales_agents]
                mongoEngine.activateUser(mongoEngine.getCollection(), userName)
                puts "Mapping #{userName}...\n"
                if userName == 'jeydurai' || userName == 'ngollagu' || userName == 'dmalkani' || userName == 'pnithin' || userName == 'guest' || 
                    userName == 'shanagar' || userName == 'jjethana' || userName == 'rashetty' || userName == 'mronad'
                    updateQuery = Hash.new()
                    updateQuery = {}
                    mongoEngine2.mapUsernameInBookingDump(mongoEngine2.getCollection(), updateQuery, userName)
                elsif userName == 'mmaniman'
                    updateQuery = Hash.new()
                    updateQuery = { "location_nodes.region" => "SOUTH"} 
                    mongoEngine2.mapUsernameInBookingDump(mongoEngine2.getCollection(), updateQuery, userName)
                elsif userName == 'timitra'
                    updateQuery = Hash.new()
                    updateQuery = { "location_nodes.region" => "WEST"} 
                    mongoEngine2.mapUsernameInBookingDump(mongoEngine2.getCollection(), updateQuery, userName)
                elsif userName == 'vimahaja' || userName == 'nidbansa'
                    updateQuery = Hash.new()
                    updateQuery = { "location_nodes.region" => "NORTH"} 
                    mongoEngine2.mapUsernameInBookingDump(mongoEngine2.getCollection(), updateQuery, userName)
                elsif userName == 'abhbaner'
                    updateQuery = Hash.new()
                    updateQuery = { :$or =>[{"location_nodes.region" => "EAST"}, {"location_nodes.region" => "SAARC"}]} 
                    mongoEngine2.mapUsernameInBookingDump(mongoEngine2.getCollection(), updateQuery, userName)
                else
                    # Loops to iterate through all possible combinations of nodes
                    salesAgents.each do |salesAgent|
                        matchHash = Hash.new()
                        groupHash = Hash.new()
                        aggregateArray= Array.new()
                        matchHash = {
                            :$match => {
                                "sales_agents" => salesAgent
                            }
                        }
                        groupHash = {
                            :$group => {
                                :_id => "$sales_level_6",
                                :recs => {
                                    :$sum => 1
                                }
                            }
                        }
                        aggregateArray= [
                            matchHash, groupHash
                        ]
                        aggreDocs = mongoEngine3.aggregateAndFetchDocuments(mongoEngine3.getCollection(), aggregateArray)
                        matchedSL6 = Array.new()
                        arrayCounter = 0 
                        tempCounter = 0
                        aggreDocs.each do |doc2|
                            if sl6Array.include? doc2[:_id]
                                matchedSL6[arrayCounter] = doc2[:_id]
                                arrayCounter += 1
                                updateQuery = Hash.new()
                                updateQuery = {
                                    "names.sales_agent.name" => salesAgent,
                                    "location_nodes.sales_level_6" => doc2[:_id]
                                }
                                #puts "Updating the Booking Data with mapping user name\n"
                                mongoEngine2.mapUsernameInBookingDump(mongoEngine2.getCollection(), updateQuery, userName)
                                #puts "Booking Data is mapped with user name\n"
                            end
                            tempCounter += 1
                        end

                        puts "#{userName} - #{salesAgent} - #{matchedSL6} - #{arrayCounter}/#{tempCounter} rec(s)\n"
                        
                    end # End of salesAgents Iteration
                end # End of If condition to check for Comm Users
                puts "#{userName} Mapped!\n"
            end # End of Enumeration each on docs
        end # End of Method authorizeAllApprovedUsers
        public :authorizeAllApprovedUsers, :authorizeApprovedUser 
    end # End of Class ActivateUsers
end # End of Module AuthroizeUsers 
