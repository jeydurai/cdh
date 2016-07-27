# Namespace, Module  and Class Loading
require './dbh/Connections'
require './dbh/QueryEngine'
include Connections
include QueryEngine



# Preparing MongoDB Connection Objects
mongoObj = Connections::MongoDBClient.new()
mongoClient = mongoObj.getMongoClient()
mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'truenorth', 'unique_nodes')

subSCMS = "PL_S"
gtmu = "EU2"
region = "WEST"
sl6 = "IN_COM_GUJ_PL_S_TM"
sa = "Sabnis,Chaitanya Madhukar"

# Documents Preparation
fullDocument = {
  subSCMS => {
    gtmu => { 
      region => { 
        sl6 => [
          sa
          ]
        }
      }
   }
}

 documentL1 = {
    gtmu => {
      region => {
        sl6 => [
          sa
        ]
      }
    }
}

documentL2 = {
     region => {
       sl6 => [
         sa
       ]
     }
}

documentL3 = {
       sl6 => [
         sa
       ]
}


# Query Preparation
field = subSCMS + "." + gtmu + "." + region + "." + sl6
field2 = subSCMS + "." + gtmu + "." + region
field3 = subSCMS + "." + gtmu
field4 = subSCMS
query  = {
  field => {"$exists" => 1}
}
query2  = {
  field2 => {"$exists" => 1}
}
query3  = {
  field3 => {"$exists" => 1}
}
query4  = {
  field4 => {"$exists" => 1}
}

puts "Documents"
puts "---------"
puts fullDocument
puts documentL1
puts documentL2
puts documentL3

puts "Queries"
puts "-------"
puts query
puts query2
puts query3
puts query4




# check if SubSCMS.GTMu.Region.Sales_Level_6 object exists
status = mongoEngine.checkIfExists(mongoEngine.getCollection(), query)
puts "Status"
puts status
case status
when 1 # If SubSCMS.GTMu.Region.Sales_Level_6 object exists
  # Prepare a document to add a new Sales Agent into the array under SubSCMS.GTMu.Region.Sales_Level_6 object  
  subDocument = {
    "$addToSet" => {field => sa
    } 
  }
  puts "Sub Document"
  puts "------------"
  puts subDocument
  # Update unique_nodes Collection
  mongoEngine.updateSubDocument(mongoEngine.getCollection(), query, subDocument)
  puts "Sales Agents Array inside SubSCMS.GTMu.Region.Sales_Level_6 object has been added!"
when 0 # Unless SubSCMS.GTMu.Region.Sales_Level_6 object exists
  # check if SubSCMS.GTMu.Region object exists
  status = mongoEngine.checkIfExists(mongoEngine.getCollection(), query2)
  case status
  when 1 # If SubSCMS.GTMu.Region object exists
    # Prepare a document to add a sub document of Sales_level_6 with its Sales Agent array under SubSCMS.GTMu.Region object  
    subDocument = {
      "$set" => {
        field => [
           sa            
         ]
      } 
    }
    puts "Sub Document"
    puts "------------"
    puts subDocument
    # Update unique_nodes Collection
    mongoEngine.updateSubDocument(mongoEngine.getCollection(), query2, subDocument)
    puts "Sales_Level_6 object inside SubSCMS.GTMu.Region object has been added!"
  when 0 # Unless SubSCMS.GTMu.Region object exists
    # check if SubSCMS.GTMu object exists
    status = mongoEngine.checkIfExists(mongoEngine.getCollection(), query3)
    case status
    when 1  # If SubSCMS.GTMu object exists
      # Prepare a document to add a sub document of Region with under SubSCMS.GTMu object  
      subDocument = {
        "$set" => {
          field2 => {
            sl6 => [
              sa
            ]
          }
        } 
      }
      puts "Sub Document"
      puts "------------"
      puts subDocument
      # Update unique_nodes Collection
      mongoEngine.updateSubDocument(mongoEngine.getCollection(), query3, subDocument)
      puts "Region object inside SubSCMS.GTMu object has been added!"
    when 0  # Unless SubSCMS.GTMu object exists
      # check if SubSCMS object exists
      status = mongoEngine.checkIfExists(mongoEngine.getCollection(), query4)
      case status
      when 1  # If SubSCMS object exists
        # Prepare a document to add a sub document of GTMu with under SubSCMS object  
        subDocument = {
          "$set" => {
            field3 => {
              region => {
                sl6 => [
                  sa
                ]
              }
            }
          } 
        }
        puts "Sub Document"
        puts "------------"
        puts subDocument
        # Update unique_nodes Collection
        mongoEngine.updateSubDocument(mongoEngine.getCollection(), query4, subDocument)
        puts "GTMu object inside SubSCMS object has been added!"
      when 0  # Unless SubSCMS object exists
        # Inserting the full Document
        id = mongoEngine.insertDocument(mongoEngine.getCollection(), fullDocument)
        puts "No Existing document found, hence, a new Full document with an ID: #{id} has been inserted!\n"
      end
    end
  end
end


