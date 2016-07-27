module Connections
  require 'Mysql2'
  require 'mongo'
  include Mongo
  
  # MySQL Connection Class
  class MySQLConnection
    attr_accessor :mysqlConnection
    def initialize()
      @mysqlConnection = Mysql2::Client.new(:host => '127.0.0.1', :username => 'root', :password => 'Jey03$78', :database => 'mysourcedata')
    end 
    
    def getConnection
      return @mysqlConnection
    end
    
    def closeConnection
      @mysqlConnection.close
    end
    
    public :getConnection, :closeConnection 
  end
  
  
  #MongoDB Connection Class
  class MongoDBClient
    attr_accessor :mongoClient
    def initialize()
      @mongoClient = Mongo::Client.new(['127.0.0.1:27017'], :database => 'truenorth', :max_pool_size => 200)
      Mongo::Logger.logger.level = ::Logger::FATAL
      Mongo::Logger.logger = ::Logger.new('mongo.log')
      Mongo::Logger.logger.level = ::Logger::INFO
    end
    
    def getMongoClient
      return @mongoClient
    end
    
    def closeMongoClient
      @mongoClient.close
    end
    
    public :getMongoClient, :closeMongoClient
  end

end
