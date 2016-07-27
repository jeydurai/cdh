module SQLToNoSQLConverter
  # Namespace, Module  and Class Loading
  require './database_handles/Connections'
  require './database_handles/QueryEngine'
  include Connections
  include QueryEngine

  class SubSetAndWrite
    attr_accessor :connObj, :queryObj, :mongoObj, :collBookingDump, :collBookingDump2

    def initialize()
      puts "Acquiring MySQL Connection....\n"
      @connObj = MySQLConnection.new()
      puts "Acquired MySQL Connection!\n"
      puts "Acquiring MySQL Query Object....\n"
      @queryObj = MySQLQuery.new()
      puts "Acquired MySQL Query Object!\n"
      puts "Acquiring MongoDB Object....\n"
      @mongoObj = Connections::MongoDBClient.new()
      puts "Acquired MongoDB Object!\n"
      @collBookingDump = 'booking_dump'
      @collBookingDump2 = 'booking_dump2'

    end 
    
    # Method to fetch all nodes from SQL and write it in MongoDB
    # in simple form
    def getAndWriteUniqueNodesAll()
      # Preparing MongoDB Connection Objects
      mongoClient = @mongoObj.getMongoClient()
      mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'unique_nodes_all')
      
      # Removing Existing Documents from unique_nodes collection
      puts "Removing Existing collection 'unique_nodes_all'..."
      mongoEngine.removeCollection(mongoEngine.getCollection())
      puts "Existing collection 'unique_nodes_all' has been removed!"

      puts "Aggregation query pattern is being prepared ..."
      mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'booking_dump')
      aggregateQuery = [
        {
            :$group => {
                :_id => {
                    :sub_scms => "$business_nodes.sub_scms",
                    :gtmu => "$location_nodes.gtmu",
                    :region => "$location_nodes.region",
                    :sales_level_6 => "$location_nodes.sales_level_6",
                    :sales_agents => "$names.sales_agent.name",
                },
                :booking => {$sum => "$metric.booking_net"}
            }
        },
        {
            :$project => {
                :_id => 0,
                :sub_scms => "$_id.sub_scms",
                :gtmu => "$_id.gtmu",
                :region => "$_id.region",
                :sales_level_6 => "$_id.sales_level_6",
                :sales_agents => "$_id.sales_agents",
                :booking => "$booking"
            }
        },
        {
            :$out => "unique_nodes_all"
        }
      ]
      puts "Preparation of Aggregation query pattern completed!!"
      # run aggregate query in booking_dump collection
      puts "Running aggregation and saving as 'unique_nodes_all' ..."
      mongoEngine.aggregateAndWriteDocuments(mongoEngine.getCollection(), aggregateQuery)
      puts "'unique_nodes_all' collection has been created by aggregation query!"
      puts "Writing Indexes on 'unique_nodes_all' collection ..."
      mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'unique_nodes_all')
      indexObj = {
          :sub_scms => 1,
          :gtmu => 1,
          :region => 1,
          :sales_level_6 => 1,
          :sales_agents => 1
      }
      mongoEngine.createIndex(mongoEngine.getCollection(), indexObj)
      indexObj = {
          :sub_scms => 1,
          :region => 1,
          :sales_level_6 => 1,
          :sales_agents => 1
      }
      mongoEngine.createIndex(mongoEngine.getCollection(), indexObj)
      indexObj = {
          :sub_scms => 1,
          :sales_level_6 => 1,
          :sales_agents => 1
      }
      mongoEngine.createIndex(mongoEngine.getCollection(), indexObj)
      indexObj = {
          :sales_level_6 => 1,
          :sales_agents => 1
      }
      mongoEngine.createIndex(mongoEngine.getCollection(), indexObj)
      indexObj = {
          :region => 1,
          :sales_level_6 => 1,
          :sales_agents => 1
      }
      mongoEngine.createIndex(mongoEngine.getCollection(), indexObj)
      puts "Writing Indexes on 'unique_nodes_all' collection completed!"
    end # End of getAndWriteUniqueNodesAll

    # Method to fetch all nodes from SQL and write it in MongoDB
    # in multi sub documents form
    def getAndWriteUniqueNodes()
      # initializing hash variables 
      hashSubSCMS = Hash.new()
      hashGTMu = Hash.new()
      hashRegion = Hash.new()
      hashSL6 = Hash.new()
      hashSalesAgent = Hash.new()
      
      # Writing Query and Obtaining Record Cursor
      puts "Preparing Query and acquiring statement cursor...\n"
      queryString = "SELECT DISTINCT sub_scms, gtmu, region, sales_level_6, tbm \
                      FROM booking_dump WHERE ((sub_scms is not null AND sub_scms <> '') AND \
                      (gtmu is not null AND gtmu <> '') AND (region is not null AND region <> '') \
                      AND (sales_level_6 is not null AND sales_level_6 <> '') AND \
                      (tbm is not null AND tbm <> '')) ORDER BY sub_scms, gtmu, region, sales_level_6, tbm"
      cursor = @queryObj.getCursor(@connObj.getConnection(), queryString)
      puts "Acquired query cursor!\n"
      puts "There are #{cursor.count} record(s) to be processed!"
      
      # Iteration through Cursor Hash
      puts "printing elements from the cursor...\n\n"
      rec_no = 0
      cursor.each_hash do |h|
        rec_no += 1     
        hashSubSCMS[rec_no] = h[:sub_scms]
        hashGTMu[rec_no] = h[:gtmu]
        hashRegion[rec_no] = h[:region]
        hashSL6[rec_no] = h[:sales_level_6]
        hashSalesAgent[rec_no] = h[:tbm]
      end # Database cursor Loop finishes here
      
      # Preparing MongoDB Connection Objects
      mongoClient = @mongoObj.getMongoClient()
      mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'unique_nodes')
      
      # Removing Existing Documents from unique_nodes collection
      mongoEngine.removeCollection(mongoEngine.getCollection())
      
      
      # Iteration through Hash Data
      (1..rec_no).each do |index|
        puts "#{hashSubSCMS[index]} | #{hashGTMu[index]} | #{hashRegion[index]} | #{hashSL6[index]} | #{hashSalesAgent[index]}"
      
        # Documents Preparation
        fullDocument = {
          hashSubSCMS[index] => {
            hashGTMu[index] => { 
              hashRegion[index] => { 
                hashSL6[index] => [
                  hashSalesAgent[index]
                  ]
                }
              }
           }
        }
      
        # Query Preparation
        field = hashSubSCMS[index] + "." + hashGTMu[index] + "." + hashRegion[index] + "." + hashSL6[index]
        field2 = hashSubSCMS[index] + "." + hashGTMu[index] + "." + hashRegion[index]
        field3 = hashSubSCMS[index] + "." + hashGTMu[index]
        field4 = hashSubSCMS[index]
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
      
            
        if index == 1
          # get the first records in 
          firstRecordSubSCMS = hashSubSCMS[index]
          firstRecordGTMu = hashGTMu[index]
          firstRecordRegion = hashRegion[index]
          firstRecordSL6 = hashSL6[index]
          firstRecordSalesAgent = hashSalesAgent[index]     
      
          # Inserting the first Document
          id = mongoEngine.insertDocument(mongoEngine.getCollection(), fullDocument)
          puts "Document ID of the first document inserted is: #{id}\n"
        end
        
        if index > 1
          # check if SubSCMS.GTMu.Region.Sales_Level_6 object exists
          status = mongoEngine.checkIfExists(mongoEngine.getCollection(), query)
          case status
          when 1 # If SubSCMS.GTMu.Region.Sales_Level_6 object exists
            # Prepare a document to add a new Sales Agent into the array under SubSCMS.GTMu.Region.Sales_Level_6 object  
            subDocument = {
              "$addToSet" => {field => hashSalesAgent[index]
              } 
            }
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
                    hashSalesAgent[index]            
                   ]
                } 
              }
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
                      hashSL6[index] => [
                        hashSalesAgent[index]
                      ]
                    }
                  } 
                }
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
                        hashRegion[index] => {
                          hashSL6[index] => [
                            hashSalesAgent[index]
                          ]
                        }
                      }
                    } 
                  }
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
        end
        
      end # Record Index Each Loop finishes here
      
      
      # Close MySQL Database Connection
      puts "\nClosing MySQL connection...\n"
      @connObj.closeConnection()
      puts "\nMySQL connection  closed! \n"
    end
    public :getAndWriteUniqueNodes
  end
  # =====================================================================================================================
  # =========================================== CONVERSION CLASS ========================================================
# =====================================================================================================================
  class ConvertAsWhole

    attr_accessor :connObj, :queryObj, :mongoObj, :collBookingDump, :collBookingDump2

    def initialize()
      puts "Acquiring MySQL Connection....\n"
      @connObj = MySQLConnection.new()
      puts "Acquired MySQL Connection!\n"
      puts "Acquiring MySQL Query Object....\n"
      @queryObj = MySQLQuery.new()
      puts "Acquired MySQL Query Object!\n"
      puts "Acquiring MongoDB Object....\n"
      @mongoObj = Connections::MongoDBClient.new()
      puts "Acquired MongoDB Object!\n"
      @collBookingDump = 'booking_dump'
      @collBookingDump2 = 'booking_dump2'
    end 


    def cleanInit() 
        # This is a Ruby script to Initialize the cleaning
        # =================================================
        # Database Details
        # ================
        # Database: mysourcedata
        # Source Table: dump_from_finance
        # tech_grand_master (get technology, arch1, arch2)

        # tech_grand_master is a sum of tech_spec(for <=FY12) and tech_spec1(for >=FY13)
        # tech_spec is TMS_Level_1 based and tech_spec1 is Bus_sub_entity based
        # While running script, either '-ri' OR '-nri' should be given as arguments
        # ========================================================================

        # ===================================================================
        # Preliminary clean up
        # ===================================================================

        paramHash = {}
        puts "Deleting tech_master Table for clean up..."
        queryString = "DELETE FROM tech_master"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "tech_master Table has been deleted!"

        puts "Inserting new data in to tech_master Table..."
        queryString = "INSERT INTO tech_master SELECT DISTINCT \
                RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) AS Tech_Code, \
                IFNULL((SELECT ts.Tech_Name_1 FROM tech_spec AS ts \
                WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts.tech_code),'Others') AS Tech_Name_1, \
                IFNULL((SELECT ts2.arch1 FROM tech_spec AS ts2 \
                WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts2.tech_code),'Other') AS arch1, \
                IFNULL((SELECT ts3.arch2 FROM tech_spec AS ts3 \
                WHERE RIGHT(TMS_Level_1_Sales_Allocated,(LENGTH(TMS_Level_1_Sales_Allocated)-LOCATE('-',TMS_Level_1_Sales_Allocated,1))) = ts3.tech_code),'Others') AS arch2 \
                FROM dump_from_finance AS df ORDER BY tech_code"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "New data in to tech_master Table has been inserted!"


        puts "Deleting tech_master1 Table for clean up..."
        queryString = "DELETE FROM tech_master1"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "tech_master1 Table has been deleted!"

        puts "Inserting new data in to tech_master1 Table..."
        queryString = "INSERT INTO tech_master1 SELECT DISTINCT Internal_Sub_Business_Entity_Name AS Tech_Code, \
                IFNULL((SELECT ts.Tech_Name_1 FROM tech_spec1 AS ts WHERE Internal_Sub_Business_Entity_Name = ts.tech_code),'Others') AS Tech_Name_1, \
                IFNULL((SELECT ts2.arch1 FROM tech_spec1 AS ts2 WHERE Internal_Sub_Business_Entity_Name = ts2.tech_code),'Others') AS arch1, \
                IFNULL((SELECT ts3.arch2 FROM tech_spec1 AS ts3 WHERE Internal_Sub_Business_Entity_Name = ts3.tech_code),'Others') AS arch2	\
                FROM dump_from_finance AS df ORDER BY tech_code"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "New data in to tech_master Table has been inserted!"


        puts "Deleting tech_grand_master Table for clean up..."
        queryString = "DELETE FROM tech_grand_master"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "tech_grand_master Table has been deleted!"

        puts "Inserting new data in to tech_grand_master Table..."
        queryString = "INSERT INTO mysourcedata.tech_grand_master SELECT * FROM mysourcedata.tech_master \
                                UNION ALL SELECT * FROM mysourcedata.tech_master1"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "New data in to tech_grand_master Table has been inserted!"

        puts "Dropping booking_dump Table..."
        queryString = "DROP TABLE booking_dump"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "booking_dump Table has been dropped!"

        puts "Dropping booking_dump_nri Table..."
        queryString = "DROP TABLE booking_dump_nri"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "booking_dump_nri Table has been dropped!"


        puts "Creating booking_dump Table..."
        queryString = "CREATE TABLE booking_dump LIKE booking_dump_template"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "booking_dump Table has been created!"

        puts "Creating booking_dump_nri Table..."
        queryString = "CREATE TABLE booking_dump_nri LIKE booking_dump_template"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        puts "booking_dump_nri Table has been created!"
        # Garbage Collection
        puts "==============================================================="
        puts "==============================================================="
        puts "\nCleaning Initialization completes!"
    end

    def convertAndWrite()
      # Hash Variable Declaration to store the objects
      paramHash = Hash.new()
      # Preparing MongoDB Connection Objects
      mongoClient = @mongoObj.getMongoClient()
      mongoEngine = QueryEngine::MongoQuery.new(mongoClient, @collBookingDump)

      # Removing All Documents in booking_dump collection
      puts "Removing old documents in booking_dump collection...\n"
      mongoEngine.removeCollection(mongoEngine.getCollection())
      puts "Old documents in booking_dump collection  have been removed successfully!\n"
            
      # Prepare QueryString to fetch all records from dump_from_finance table
      queryString = "SELECT * FROM dump_from_finance WHERE Booking_Net<>0"
      # Get a query Cursor for all the fetched records of dump_from_finance table
      puts "Acquiring Cursor from Mysql for all the dump_from_finance table records...\n"
      mysqlCursor = @queryObj.getCursor(@connObj.getConnection(), queryString)
      puts "Acquired Cursor from Mysql for all the dump_from_finance table records!\n"
      
      # Iterate over the fetched dump_from_finance table cursor
      loopCounter = 0
      totalCursors = mysqlCursor.count
      puts "Total number of Cursors #{totalCursors}\n"
      puts "Entering the dump_from_finance table iteration loop...\n"
      
      startTime = Time.now
      print_timer = 1 # Print breaker to ensure for every interval only one time the info gets printed
      prev_exe_time = 0 # Variable to store previous Execution Time for print breaker 
      prev_so_far_data = 1 # Variable to store previous intervals data processed
      
      mysqlCursor.each_hash do |h| #Cursor loop begins
        #puts "Initializing Record No.: #{loopCounter} ...\n"
        # Organizing and assigning all period details in their respective variables
        # =========================================================================
        fiscal_period_id = h['Fiscal_Period_ID']
        fiscal_quarter_id = h['Fiscal_Quarter_ID']
        fiscal_week_id = h['Fiscal_Week_ID']
        prod_ser = h['prod_ser']
        period_year = fiscal_period_id[0, 4] # First four characters in fiscal_period_id is four digits numerical year
        period_month = fiscal_period_id[-2, 2] # Last two characters in fiscal_period_id is two digits numerical month
        period_quarter = fiscal_quarter_id[-2, 2] # Last two characters in fiscal_quarter_id is two characters quarter string (Q1, Q2..)
        period_week = ''
        week_field_last_two = fiscal_week_id[-2, 2] # Substring last two characters from fiscal_week_id
        paramHash['param1'] = period_quarter # Prepare MySQL parameter-1
        paramHash['param2'] = week_field_last_two # Prepare MySQL parameter-2
        queryString = "SELECT fp_week FROM week_master WHERE fp_quarter=? AND week_in_database=?" # Query String to fetch proper week strings from week_master table
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash) # Receive cursor from the week master by passing parameters
        
        # Iterate over cursor from week_master table
        cursor.each_hash do |rec_hash| # week_master query cursor iteration begins
          period_week = rec_hash['fp_week'] # Assign the proper week string in a variable
        end # week_master query cursor iteration ends
        cursor.close()
        
        # =========================================================================
        
        # Unique Customer Name Extraction
        # =========================================================================
        account_name = h['Customer_Name'] # Assign the account_name in a variable from the dump_from_finanace table query cursor
        customer_name = '' 
        vertical =''
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
        paramHash['param1'] = account_name # Assign parameter
        queryString = "SELECT DISTINCT unique_names, vertical FROM universal_unique_names WHERE names=?"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        
        cursor.each_hash do |rec_hash|
          customer_name = rec_hash['unique_names']
          vertical = rec_hash['vertical']
        end
        cursor.close()
        # =========================================================================
        
        # Unique Partner Name Extraction
        # =========================================================================
        partner = h['Partner_Name'] # Assign the non unique partner name in a variable from the dump_from_finance table query cursor
        partner_name = ''
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
        paramHash['param1'] = partner
        queryString = "SELECT DISTINCT unique_names FROM universal_unique_names WHERE names=?"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        
        cursor.each_hash do |rec_hash|
          partner_name = rec_hash['unique_names']
        end
        cursor.close()

        # =========================================================================
        
        # Unique States Extraction
        # =========================================================================
        sl6 = h['Sales_Level_6'] # Assign the sales_level_6 in a variable from the dump_from_finance table query cursor
        unique_state = ''
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
        paramHash['param1'] = sl6
        queryString = "SELECT DISTINCT unique_states FROM unique_states WHERE sales_level_6=?"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        
        cursor.each_hash do |rec_hash|
          unique_state = rec_hash['unique_states']
        end
        cursor.close()

        # =========================================================================
        # Region & GTMu Extraction from Sales_Level_5
        # =========================================================================
        sales_level_5 = h['Sales_Level_5']
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
        paramHash['param1'] = sales_level_5
        gtmu = ""
        region = ""
        queryString = "SELECT region, gtmu FROM node_mapper WHERE sales_level_5 = ?"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        
        cursor.each_hash do |rec_hash|
          gtmu = rec_hash['gtmu']
          region = rec_hash['region']
        end
        if (not gtmu) or (gtmu == "")
          gtmu = 'COMM'
        end
        if (not region) or (region == "")
          region = 'COMM'
        end
        cursor.close()
        # =========================================================================

        # =========================================================================
        # Sub SCMS Extraction from Sub_SCMS
        # =========================================================================
        sub_scms = h['Sub_SCMS']
        if regex_match = sub_scms.match(/(PL_S|PL|COM-OTHER|COM-MM|GEO_NAMED|GEO_NON_NA|SELECT)/i)
          sub_scms = regex_match.captures[0]
        else
          sub_scms = 'COM-OTHER'
        end
        # =========================================================================

        # Technology Name Extraction
        # =========================================================================
        tms_level_1 = h['TMS_Level_1_Sales_Allocated'] # Assign the TMS_Level_1 in a variable from the dump_from_finance table query cursor
        tech_code = h['Internal_Sub_Business_Entity_Name'] # Assign Internal_Business_Sub_Entity in a variable from the dump_from_finance table query cursor
        tech_name = ''
        arch1 = ''
        arch2 = ''
        if period_year.to_i <= 2012
          tech_code = tms_level_1[tms_level_1.index("-")+1, tms_level_1.length]
        end
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
        paramHash['param1'] = tech_code
        queryString = "SELECT DISTINCT tech_name_1, arch1, arch2 FROM tech_grand_master WHERE tech_code=?"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        
        cursor.each_hash do |rec_hash|
          tech_name = rec_hash['tech_name_1']
          arch1 = rec_hash['arch1']
          arch2 = rec_hash['arch2']
        end
        cursor.close()
        sales_level_6 = h['Sales_Level_6'] # Assign the sales_level_6 in a variable from the dump_from_finance table query cursor
        sales_agent_name = h['TBM'] # Assign the sales_agent_name in a variable from the dump_from_finance table query cursor
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
   
        # =========================================================================
        # IOT Portfolio
        # =========================================================================
        product_family = h['Product_Family'] # Assign Product Family in a variable from the dump_from_finance table query cursor
        product_id = h['Product_ID'] # Assign the Product ID in a variable from the dump_from_finance table query cursor
        iot_portfolio = ''
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
        paramHash['param1'] = product_family
        paramHash['param2'] = product_id
          
        queryString = "SELECT iot_portfolio FROM iot_portfolios WHERE Product_Fam_id = ? OR Product_Fam_id = ?"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        
        cursor.each_hash do |rec_hash|
          iot_portfolio = rec_hash["iot_portfolio"];
        end
        cursor.close()
        # =========================================================================
        
        # Final Object Preparation
        # =========================================================================
        finalDoc = {
          :booking_adjustments => {
            :bookings_adjustments_code => h['Bookings_Adjustments_Code'],
            :bookings_adjustments_description => h['Bookings_Adjustments_Description'],
            :bookings_adjustments_type => h['Bookings_Adjustments_Type'],
            :cbn_flag => h['CBN_Flag']
          },
          :periods => {
            :year => period_year,
            :quarter => period_quarter,
            :month => period_month,
            :week => period_week
          },
          :names => {
            :partner => {
              :name => partner,
              :unique_name => partner_name,
              :tier_code => h['Partner_Tier_Code'],
              :certification => h['Partner_Certification'],
              :type => h['Partner_Type']
            },
            :customer => {
              :name => account_name,
              :unique_name => customer_name
            },
            :sales_agent => {
              :name => sales_agent_name,
            }
          },
          :technologies => {
            :product_family => h['Product_Family'],
            :product_id => h['Product_ID'],
            :technology_group => h['Technology_Group'],
            :tms_level_1_sales_allocated => h['TMS_Level_1_Sales_Allocated'],
            :tms_level_2_sales_allocated => h['TMS_Level_2_Sales_Allocated'],
            :tms_level_3_sales_allocated => h['TMS_Level_3_Sales_Allocated'],
            :internal_business_entity_name => h['Internal_Business_Entity_Name'],
            :internal_sub_business_entity_name => h['Internal_Sub_Business_Entity_Name'],
            :tech_name => tech_name,
            :arch1 => arch1,
            :arch2 => arch2,
            :at_attach => h['AT_Attach'],
            :iot_portfolio => iot_portfolio
          },
          :business_nodes => {
            :sales_level_3 => h['Sales_Level_3'],
            :scms => h['SCMS'],
            :sub_scms => sub_scms,
            :business_unit => h['Business_Unit'],
            :industry_vertical => vertical, 
          },
          :location_nodes => {
            :sales_level_4 => h['Sales_Level_4'],
            :gtmu => gtmu,
            :sales_level_5 => h['Sales_Level_5'],
            :region => region,
            :sales_level_6 => sales_level_6,
            :unique_state => unique_state,
            :bill_to_site_city => h['Bill_To_Site_City'],
            :ship_to_city => h['Ship_To_City']
          },
          :references => {
            :erp_deal_id => h['ERP_Deal_ID'],
            :sales_order_number_detail => h['Sales_Order_Number_Detail']
          },
          :metric => {
            :booking_net => h['Booking_Net'].to_f,
            :base_list => h['TMS_Sales_Allocated_Bookings_Base_List'].to_f,
            :standard_cost => h['standard_cost'].to_f,
            :bookings_quantity => h['TMS_Sales_Allocated_Bookings_Quantity'].to_f
          },
          :prod_ser => prod_ser
        }
        # Final Object Preparation Ends
        # =========================================================================
        id = mongoEngine.insertDocument(mongoEngine.getCollection(), finalDoc)
       
        # =========================================================================
        # Write data in MySQL
        # =========================================================================
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
        paramHash['param1'] = h['ID']; paramHash['param2'] = h['AT_Attach']; paramHash['param3'] = account_name; paramHash['param4'] = customer_name; paramHash['param5'] = h['ERP_Deal_ID']; 
        paramHash['param6'] = h['Sales_Order_Number_Detail']; paramHash['param7'] = period_year; paramHash['param8'] = period_quarter; paramHash['param9'] = period_month; paramHash['param10'] = period_week; 
        paramHash['param11'] = partner; paramHash['param12'] = partner_name; paramHash['param13'] = sales_agent_name; paramHash['param14'] = region; paramHash['param15'] = sales_level_6; 
        paramHash['param16'] = h['SCMS']; paramHash['param17'] = sub_scms; paramHash['param18'] = h['TMS_Level_1_Sales_Allocated']; paramHash['param19'] = tech_name; paramHash['param20'] = tech_code; 
        paramHash['param21'] = h['Technology_Group']; paramHash['param22'] = h['Partner_Tier_Code']; paramHash['param23'] = h['Ship_To_City']; paramHash['param24'] = h['Booking_Net']; paramHash['param25'] = h['TMS_Sales_Allocated_Bookings_Base_List']; 
        paramHash['param26'] = h['TMS_Sales_Allocated_Bookings_Quantity']; paramHash['param27'] = h['Internal_Business_Entity_Name']; paramHash['param28'] = h['Internal_Sub_Business_Entity_Name']; paramHash['param29'] = arch1; paramHash['param30'] = arch2; 
        paramHash['param31'] = h['Product_ID']; paramHash['param32'] = h['Bill_To_Site_City']; paramHash['param33'] = vertical; paramHash['param34'] = iot_portfolio; paramHash['param35'] = gtmu; 
        paramHash['param36'] = h['Product_Family']; paramHash['param37'] = h['Bookings_Adjustments_Type']; paramHash['param38'] = h['Partner_Certification']; paramHash['param39'] = h['Partner_Type']; paramHash['param40'] = unique_state; 
        paramHash['param41'] = prod_ser; paramHash['param42'] = h['standard_cost'];
        queryString = "INSERT INTO booking_dump \
                        (ID, At_Attach, Account_Name, Customer_Name, ERP_Deal_ID, Sales_Order_Number_Detail, \
                        FP_Year, FP_Quarter, FP_Month, FP_Week, Partner, Partner_Name, TBM, Region, Sales_Level_6, \
                        SCMS, Sub_SCMS, TMS_Level_1_Sales_Allocated, Tech_Name, Tech_Code, Technology_Group, \
                        Partner_Tier_Code, Ship_To_City, Booking_Net, Base_List, TMS_Sales_Allocated_Bookings_Quantity, \
                        Internal_Business_Entity_Name, Internal_Sub_Business_Entity_Name, arch1, arch2, Product_ID, \
                        Bill_To_Site_City, Vertical, iot_portfolio, GTMu, Product_Family, Booking_Adjustment, Partner_Certification, \
                        Partner_Type, unique_state, prod_ser, standard_cost
                        ) \
                        VALUES \
                        ( \
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                            ?, ? \
                        )"
        cursor = @queryObj.getCursorByParameters(@connObj.getConnection(), queryString, paramHash)
        cursor.close()
        paramHash.clear() # Just before re-using paramHash hash variable, clear the old contents
        # =========================================================================
        
        # ==============================================================
        # Processing Time Handler
        # ==============================================================
        inbetween = Time.now # Record current time inside the loop
        exe_time = inbetween - startTime # Difference between initial time and current time
        loopCounter += 1
        if prev_exe_time.to_i != exe_time.to_i  # Trigger $print_timer to 1 if the execution time has changed to new interval
          print_timer = 1
        end
        
        if ((exe_time % 5).to_i == 0) and (print_timer == 1)  # Check whether the same interval is repeated and print the processing information accordingly  
          process_speed_in_recs = ((loopCounter-prev_so_far_data+1).to_f/5).to_f # Processing speed - No. Records per Second
          process_speed_in_secs = (5/(loopCounter-prev_so_far_data+1).to_f).to_f # Processing speed - No. Seconds per Record
          exp_secs = process_speed_in_secs*(totalCursors-loopCounter+1).to_f; # Expected Time in seconds
          exp_hour = (exp_secs/(60*60)).to_i  # Expected Time in Hours
          exp_min = (exp_secs/60).to_i  # Expected Time in Minutes
          prev_so_far_data = loopCounter; # Current so far data is stored as Previous So far data
          # Printing the Processing Information
          puts "Elapsed #{exe_time.round(2)} secs || processed: #{loopCounter}/#{totalCursors} @#{process_speed_in_recs.round(2)} DPS|#{process_speed_in_secs.round(2)} SPD || ETC: #{exp_hour} hr(s)|#{exp_min} min(s)|#{exp_secs.round(2)} sec(s)\n"
          prev_exe_time = exe_time;
          print_timer = 0; # $print_timer is made 0 so that in a definite interval, the information is not repeatedly printed
        end
      end # End of MySQL Cursor on dump_from_finance table
      endTime = Time.now # Record the End Time
      final_secs = endTime - startTime # Difference between initial time and current time
      final_hour = (final_secs/(60*60)).to_i  # Expected Time in Hours
      final_min = (final_secs/60).to_i  # Expected Time in Minutes
      puts "\n\nTotal time elapsed to update 'booking_dump collection': #{final_hour} hr(s) | #{final_min} min(s) | #{final_secs.round(2)} sec(s)"
    end # End of ConvertAndWrite Method


    def convertAsSnapshotAndWrite()
      # Hash Variable Declaration to store the objects
      paramHash = Hash.new()
      # Preparing MongoDB Connection Objects
      mongoClient = @mongoObj.getMongoClient()
      mongoEngine = QueryEngine::MongoQuery.new(mongoClient, @collBookingDump2)

      # Removing All Documents in booking_dump collection
      puts "Removing old documents in booking_dump collection...\n"
      mongoEngine.removeCollection(mongoEngine.getCollection())
      puts "Old documents in booking_dump collection  have been removed successfully!\n"
            
      # Prepare QueryString to fetch all grouped records from booking_dump table
      queryString = %{SELECT DISTINCT 
                        AT_Attach,
                        Account_Name,
                        Customer_Name,
                        ERP_Deal_ID,
                        Sales_Order_Number_Detail,
                        FP_Year, 
                        FP_Quarter,
                        FP_Month,
                        FP_Week,
                        partner,
                        Partner_Name,
                        TBM,
                        region,
                        Sales_Level_6,
                        unique_state,
                        SCMS,
                        Sub_SCMS,
                        Tech_Name,
                        Partner_Tier_Code,
                        arch1,
                        arch2,
                        Vertical,
                        iot_portfolio,
                        GTMu,
                        Partner_Certification,
                        prod_ser,
                        SUM(Booking_Net) AS Booking_Net,
                        SUM(Base_List) As Base_List,
                        SUM(standard_cost) As standard_cost
                    FROM 
                        mysourcedata.booking_dump
                    GROUP BY
                        AT_Attach,
                        Account_Name,
                        Customer_Name,
                        ERP_Deal_ID,
                        Sales_Order_Number_Detail,
                        FP_Year, 
                        FP_Quarter,
                        FP_Month,
                        FP_Week,
                        partner,
                        Partner_Name,
                        TBM,
                        region,
                        Sales_Level_6,
                        unique_state,
                        SCMS, Sub_SCMS,
                        Tech_Name,
                        Partner_Tier_Code,
                        arch1,
                        arch2,
                        Vertical,
                        iot_portfolio,
                        GTMu,
                        Partner_Certification,
                        prod_ser
        }
      # Get a query Cursor for all the fetched records of dump_from_finance table
      puts "Acquiring Cursor from Mysql for all the booking_dump table records...\n"
      mysqlCursor = @queryObj.getCursor(@connObj.getConnection(), queryString)
      puts "Acquired Cursor from Mysql for all the dump_from_finance table records!\n"
      
      # Iterate over the fetched dump_from_finance table cursor
      loopCounter = 0
      totalCursors = mysqlCursor.count
      puts "Total number of Cursors #{totalCursors}\n"
      puts "Entering the dump_from_finance table iteration loop...\n"
      
      startTime = Time.now
      print_timer = 1 # Print breaker to ensure for every interval only one time the info gets printed
      prev_exe_time = 0 # Variable to store previous Execution Time for print breaker 
      prev_so_far_data = 1 # Variable to store previous intervals data processed
      
      mysqlCursor.each do |h| #Cursor loop begins
        #puts "Initializing Record No.: #{loopCounter} ...\n"
        
        # Final Object Preparation
        # =========================================================================
        finalDoc = {
          :periods => {
            :year => h['FP_Year'],
            :quarter => h['FP_Quarter'],
            :month => h['FP_Month'],
            :week => h['FP_Week']
          },
          :names => {
            :partner => {
              :name => h['partner'],
              :unique_name => h['Partner_Name'],
              :tier_code => h['Partner_Tier_Code'],
              :certification => h['Partner_Certification'],
            },
            :customer => {
              :name => h['Account_Name'],
              :unique_name => h['Customer_Name']
            },
            :sales_agent => {
              :name => h['TBM']
            }
          },
          :technologies => {
            :tech_name => h['Tech_Name'],
            :arch1 => h['arch1'],
            :arch2 => h['arch2'],
            :at_attach => h['AT_Attach'],
            :iot_portfolio => h['iot_portfolio']
          },
          :business_nodes => {
            :scms => h['SCMS'],
            :sub_scms => h['Sub_SCMS'],
            :industry_vertical => h['Vertical'],
          },
          :location_nodes => {
            :gtmu => h['GTMu'],
            :region => h['region'],
            :sales_level_6 => h['Sales_Level_6'],
            :unique_state => h['unique_state'],
          },
          :references => {
            :erp_deal_id => h['ERP_Deal_ID'],
            :sales_order_number_detail => h['Sales_Order_Number_Detail']
          },
          :metric => {
            :booking_net => h['Booking_Net'].to_f,
            :base_list => h['Base_List'].to_f,
            :standard_cost => h['standard_cost'].to_f,
          },
          :prod_ser => h['prod_ser']
        }
        # Final Object Preparation Ends
        # =========================================================================
        id = mongoEngine.insertDocument(mongoEngine.getCollection(), finalDoc)
        
        
        # ==============================================================
        # Processing Time Handler
        # ==============================================================
        inbetween = Time.now # Record current time inside the loop
        exe_time = inbetween - startTime # Difference between initial time and current time
        loopCounter += 1
        if prev_exe_time.to_i != exe_time.to_i  # Trigger $print_timer to 1 if the execution time has changed to new interval
          print_timer = 1
        end
        
        if ((exe_time % 5).to_i == 0) and (print_timer == 1)  # Check whether the same interval is repeated and print the processing information accordingly  
          process_speed_in_recs = ((loopCounter-prev_so_far_data+1).to_f/5).to_f # Processing speed - No. Records per Second
          process_speed_in_secs = (5/(loopCounter-prev_so_far_data+1).to_f).to_f # Processing speed - No. Seconds per Record
          exp_secs = process_speed_in_secs*(totalCursors-loopCounter+1).to_f; # Expected Time in seconds
          exp_hour = (exp_secs/(60*60)).to_i  # Expected Time in Hours
          exp_min = (exp_secs/60).to_i  # Expected Time in Minutes
          prev_so_far_data = loopCounter; # Current so far data is stored as Previous So far data
          # Printing the Processing Information
          puts "Elapsed #{exe_time.round(2)} secs || processed: #{loopCounter}/#{totalCursors} @#{process_speed_in_recs.round(2)} DPS|#{process_speed_in_secs.round(2)} SPD || ETC: #{exp_hour} hr(s)|#{exp_min} min(s)|#{exp_secs.round(2)} sec(s)\n"
          prev_exe_time = exe_time;
          print_timer = 0; # $print_timer is made 0 so that in a definite interval, the information is not repeatedly printed
        end
      end # End of MySQL Cursor on dump_from_finance table
      endTime = Time.now # Record the End Time
      final_secs = endTime - startTime # Difference between initial time and current time
      final_hour = (final_secs/(60*60)).to_i  # Expected Time in Hours
      final_min = (final_secs/60).to_i  # Expected Time in Minutes
      puts "\n\nTotal time elapsed to update 'booking_dump collection': #{final_hour} hr(s) | #{final_min} min(s) | #{final_secs.round(2)} sec(s)"
    end # End of ConvertAndWrite Method

   public :convertAndWrite, :convertAsSnapshotAndWrite

  end # End of ConvertAsWhole class
end # End of Module
