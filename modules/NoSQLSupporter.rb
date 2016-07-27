module NoSQLSupporter
  # Namespace, Module  and Class Loading
  require './database_handles/Connections'
  require './database_handles/QueryEngine'
  include Connections
  include QueryEngine
  
  class IndexHandler

    def initialize()
      puts "Acquiring MongoDB Object....\n"
      @mongoObj = Connections::MongoDBClient.new()
      puts "Acquired MongoDB Object!\n"
    end 

    def removeAllIndexes()
      start_time = Time.now
      puts "Acquiring Mongo Engine...\n"
      mongoClient = @mongoObj.getMongoClient()
      mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'truenorth', 'booking_dump')
      puts "Mongo Engine has been acquired!\n"
      
      # Dropping all indexes
      puts "Removing All Indexes...\n"
      mongoEngine.removeIndexes(mongoEngine.getCollection())
      puts "All Indexes have been removed successfully!\n"
    end

    def createAllIndexes()
      start_time = Time.now
      puts "Acquiring Mongo Engine...\n"
      mongoClient = @mongoObj.getMongoClient()
      mongoEngine = QueryEngine::MongoQuery.new(mongoClient, 'truenorth', 'booking_dump')
      puts "Mongo Engine has been acquired!\n"
      
      # Create Index for Booking_Adjustments
      changing_starting_time = Time.now
      puts "Creating Index on booking_adjustments.bookings_adjustments_type...\n"
      indexObject = {"booking_adjustments.bookings_adjustments_type" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on booking_adjustments.bookings_adjustments_type has been created successfully!\n"
      changing_end_time = Time.now
      time_elapsed_secs = changing_end_time-changing_starting_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "Elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      
      # Create Index for Periods
      changing_starting_time = Time.now
      puts "Creating Index on periods.year...\n"
      indexObject = {"periods.year" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on periods.year has been created successfully!\n"
      puts "Creating Index on periods.quarter...\n"
      indexObject = {"periods.quarter" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on periods.quarter has been created successfully!\n"
      puts "Creating Index on periods.month...\n"
      indexObject = {"periods.month" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on periods.month has been created successfully!\n"
      puts "Creating Index on periods.week...\n"
      indexObject = {"periods.week" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on periods.week has been created successfully!\n"
      puts "Creating Compound Index on periods.year|quarter|month|week...\n"
      indexObject = {"periods.year" => 1, "periods.quarter" => 1, "periods.month"  => 1, "periods.week"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Compound Index on periods.year|quarter|month|week has been created successfully!\n"
      changing_end_time = Time.now
      time_elapsed_secs = changing_end_time-changing_starting_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "Elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      time_elapsed_secs = changing_end_time-start_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "So far elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"

      # Create Index for Names
      changing_starting_time = Time.now
      #puts "Creating Index on names.partner...\n"
      #indexObject = {"names.partner.name" => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on names.partner has been created successfully!\n"
      puts "Creating Index on names.partner.unique_name...\n"
      indexObject = {"names.partner.unique_name" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.partner.unique_name has been created successfully!\n"
      #puts "Creating Compound Index on names.partner.name|unique_name...\n"
      #indexObject = {"names.partner.name" => 1, "names.partner.unique_name" => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on names.partner.name|unique_name has been created successfully!\n"
      puts "Creating Index on names.partner.tier_code...\n"
      indexObject = {"names.partner.tier_code"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.partner.tier_code has been created successfully!\n"
      puts "Creating Index on names.partner.certification...\n"
      indexObject = {"names.partner.certification" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.partner.certification has been created successfully!\n"
      #puts "Creating Index on names.customer.name...\n"
      #indexObject = {"names.customer.name" => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on names.customer.name has been created successfully!\n"
      puts "Creating Index on names.customer.unique_name...\n"
      indexObject = {"names.customer.unique_name" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.customer.unique_name has been created successfully!\n"
      #puts "Creating Compound Index on names.customer.name|unique_name...\n"
      #indexObject = {"names.customer.name"  => 1, "names.customer.unique_name"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on names.customer.name|unique_name has been created successfully!\n"
      #puts "Creating Compound Index on names.customer.fy15_tam...\n"
      #indexObject = {"names.customer.fy15_tam" => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on names.customer.fy15_tam has been created successfully!\n"
      #puts "Creating Compound Index on names.partner.name|customer.name...\n"
      #indexObject = {"names.partner.name" => 1, "names.customer.name" => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on names.partner.name|customer.name has been created successfully!\n"
      puts "Creating Compound Index on names.partner.unique_name|customer.unique_name...\n"
      indexObject = {"names.partner.unique_name" => 1, "names.customer.unique_name" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Compound Index on names.partner.unique_name|customer.unique_name has been created successfully!\n"
      puts "Creating Index on names.sales_agent.name...\n"
      indexObject = {"names.sales_agent.name"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.name has been created successfully!\n"
      puts "Creating Compound Index on names.partner.unique_name|customer.unique_name|sales_agent.name...\n"
      indexObject = {"names.partner.unique_name"  => 1, "names.customer.unique_name" => 1, "names.sales_agent.name"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Compound Index on names.partner.unique_name|customer.unique_name|sales_agent.name has been created successfully!\n"
      puts "Creating Index on names.sales_agent.mapped.id_l5...\n"
      indexObject = {"names.sales_agent.mapped.id_l5"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.id_l5 has been created successfully!\n"
      puts "Creating Index on names.sales_agent.mapped.name_l5...\n"
      indexObject = {"names.sales_agent.mapped.name_l5"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.name_l5 has been created successfully!\n"
      puts "Creating Index on types.sales_agent.mapped.type_l5...\n"
      indexObject = {"types.sales_agent.mapped.type_l5"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on types.sales_agent.mapped.type_l5 has been created successfully!\n"

      puts "Creating Index on names.sales_agent.mapped.id_l4...\n"
      indexObject = {"names.sales_agent.mapped.id_l4"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.id_l4 has been created successfully!\n"
      puts "Creating Index on names.sales_agent.mapped.name_l4...\n"
      indexObject = {"names.sales_agent.mapped.name_l4"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.name_l4 has been created successfully!\n"
      puts "Creating Index on types.sales_agent.mapped.type_l4...\n"
      indexObject = {"types.sales_agent.mapped.type_l4"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on types.sales_agent.mapped.type_l4 has been created successfully!\n"

      puts "Creating Index on names.sales_agent.mapped.id_l3...\n"
      indexObject = {"names.sales_agent.mapped.id_l3"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.id_l3 has been created successfully!\n"
      puts "Creating Index on names.sales_agent.mapped.name_l3...\n"
      indexObject = {"names.sales_agent.mapped.name_l3"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.name_l3 has been created successfully!\n"
      puts "Creating Index on types.sales_agent.mapped.type_l3...\n"
      indexObject = {"types.sales_agent.mapped.type_l3"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on types.sales_agent.mapped.type_l3 has been created successfully!\n"
    
      puts "Creating Index on names.sales_agent.mapped.id_l2...\n"
      indexObject = {"names.sales_agent.mapped.id_l2"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.id_l2 has been created successfully!\n"
      puts "Creating Index on names.sales_agent.mapped.name_l2...\n"
      indexObject = {"names.sales_agent.mapped.name_l2"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.name_l2 has been created successfully!\n"
      puts "Creating Index on types.sales_agent.mapped.type_l2...\n"
      indexObject = {"types.sales_agent.mapped.type_l2"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on types.sales_agent.mapped.type_l2 has been created successfully!\n"

      puts "Creating Index on names.sales_agent.mapped.id_l1...\n"
      indexObject = {"names.sales_agent.mapped.id_l1"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.id_l1 has been created successfully!\n"
      puts "Creating Index on names.sales_agent.mapped.name_l1...\n"
      indexObject = {"names.sales_agent.mapped.name_l1"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.name_l1 has been created successfully!\n"
      puts "Creating Index on types.sales_agent.mapped.type_l1...\n"
      indexObject = {"types.sales_agent.mapped.type_l1"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on types.sales_agent.mapped.type_l1 has been created successfully!\n"

      puts "Creating Index on names.sales_agent.mapped.id_l0...\n"
      indexObject = {"names.sales_agent.mapped.id_l0"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.id_l0 has been created successfully!\n"
      puts "Creating Index on names.sales_agent.mapped.name_l0...\n"
      indexObject = {"names.sales_agent.mapped.name_l0"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on names.sales_agent.mapped.name_l0 has been created successfully!\n"
      puts "Creating Index on types.sales_agent.mapped.type_l0...\n"
      indexObject = {"types.sales_agent.mapped.type_l0"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on types.sales_agent.mapped.type_l0 has been created successfully!\n"
      changing_end_time = Time.now
      time_elapsed_secs = changing_end_time-changing_starting_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "Elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      time_elapsed_secs = changing_end_time-start_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "So far elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      
      # Create Index for Technologies
      changing_starting_time = Time.now
      puts "Creating Index on technologies.product_family...\n"
      indexObject = {"technologies.product_family"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on technologies.product_family has been created successfully!\n"
      puts "Creating Index on technologies.product_id...\n"
      indexObject = {"technologies.product_id"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on technologies.product_id has been created successfully!\n"
      puts "Creating Compound Index on technologies.product_family|product_id...\n"
      indexObject = {"technologies.product_family"  => 1, "technologies.product_id"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Compound Index on technologies.product_family|product_id has been created successfully!\n"
      #puts "Creating Index on technologies.tms_level_1_sales_allocated...\n"
      #indexObject = {"technologies.tms_level_1_sales_allocated"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on technologies.tms_level_1_sales_allocated has been created successfully!\n"
      #puts "Creating Index on technologies.tms_level_2_sales_allocated...\n"
      #indexObject = {"technologies.tms_level_2_sales_allocated"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on technologies.tms_level_2_sales_allocated has been created successfully!\n"
      #puts "Creating Index on technologies.tms_level_3_sales_allocated...\n"
      #indexObject = {"technologies.tms_level_3_sales_allocated"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on technologies.tms_level_3_sales_allocated has been created successfully!\n"
      #puts "Creating Compound Index on technologies.tms_level_1_sales_allocated|level_2|level3...\n"
      #indexObject = {"technologies.tms_level_1_sales_allocated"  => 1, "technologies.tms_level_2_sales_allocated"  => 1, "technologies.tms_level_3_sales_allocated"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on technologies.tms_level_1_sales_allocated|level_2|level3 has been created successfully!\n"
      #puts "Creating Index on technologies.internal_business_entity_name...\n"
      #indexObject = {"technologies.internal_business_entity_name"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on technologies.internal_business_entity_name has been created successfully!\n"
      #puts "Creating Index on technologies.internal_sub_business_entity_name...\n"
      #indexObject = {"technologies.internal_sub_business_entity_name"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on technologies.internal_sub_business_entity_name has been created successfully!\n"
      puts "Creating Index on technologies.tech_name...\n"
      indexObject = {"technologies.tech_name"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on technologies.tech_name has been created successfully!\n"

      puts "Creating Index on technologies.arch1...\n"
      indexObject = {"technologies.arch1" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on technologies.arch1 has been created successfully!\n"
      puts "Creating Index on technologies.arch2...\n"
      indexObject = {"technologies.arch2" => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on technologies.arch2 has been created successfully!\n"
      
      #puts "Creating Compound Index on technologies.tech_name|arch2...\n"
      #indexObject = {"technologies.tech_name"  => 1, "technologies.arch2"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on technologies.tech_name|arch2 has been created successfully!\n"
      #puts "Creating Compound Index on technologies.arch1|arch2...\n"
      #indexObject = {"technologies.arch1"  => 1, "technologies.arch2"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on technologies.arch1|arch2 has been created successfully!\n"
      #puts "Creating Compound Index on technologies.tech_name|arch1|arch2...\n"
      #indexObject = {"technologies.tech_name"  => 1, "technologies.arch1"  => 1,"technologies.arch2"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on technologies.tech_name|arch1|arch2 has been created successfully!\n"
      #puts "Creating Compound Index on technologies.tech_name|arch1...\n"
      #indexObject = {"technologies.tech_name"  => 1, "technologies.arch1"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Compound Index on technologies.tech_name|arch1 has been created successfully!\n"

      puts "Creating Index on technologies.at_attach...\n"
      indexObject = {"technologies.at_attach"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on technologies.at_attach has been created successfully!\n"
      puts "Creating Index on technologies.iot_portfolio...\n"
      indexObject = {"technologies.iot_portfolio"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on technologies.iot_portfolio has been created successfully!\n"
      changing_end_time = Time.now
      time_elapsed_secs = changing_end_time-changing_starting_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "Elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      time_elapsed_secs = changing_end_time-start_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "So far elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"

      # Create Index for Business Nodes
      changing_starting_time = Time.now
      #puts "Creating Index on business_nodes.sales_level_3...\n"
      #indexObject = {"business_nodes.sales_level_3"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on business_nodes.sales_level_3 has been created successfully!\n"
      #puts "Creating Index on business_nodes.scms...\n"
      #indexObject = {"business_nodes.scms"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on business_nodes.scms has been created successfully!\n"
      puts "Creating Index on business_nodes.sub_scms...\n"
      indexObject = {"business_nodes.sub_scms"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.sub_scms has been created successfully!\n"
      puts "Creating Index on business_nodes.mapped_sub_scms...\n"
      indexObject = {"business_nodes.mapped_sub_scms"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.mapped_sub_scms has been created successfully!\n"
      puts "Creating Index on business_nodes.industry_vertical...\n"
      indexObject = {"business_nodes.industry_vertical"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.industry_vertical has been created successfully!\n"
      changing_end_time = Time.now
      time_elapsed_secs = changing_end_time-changing_starting_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "Elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      time_elapsed_secs = changing_end_time-start_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "So far elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"

      #Create Index for Location Nodes
      changing_starting_time = Time.now
      #puts "Creating Index on business_nodes.sales_level_4...\n"
      #indexObject = {"location_nodes.sales_level_4"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on business_nodes.sales_level_4 has been created successfully!\n"
      puts "Creating Index on business_nodes.gtmu...\n"
      indexObject = {"location_nodes.gtmu"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.gtmu has been created successfully!\n"
      puts "Creating Index on business_nodes.mapped_gtmu...\n"
      indexObject = {"location_nodes.mapped_gtmu"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.mapped_gtmu has been created successfully!\n"
      #puts "Creating Index on business_nodes.sales_level_5...\n"
      #indexObject = {"location_nodes.sales_level_5"  => 1}
      #mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      #puts "Index on business_nodes.sales_level_5 has been created successfully!\n"
      puts "Creating Index on business_nodes.region...\n"
      indexObject = {"location_nodes.region"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.region has been created successfully!\n"
      puts "Creating Index on business_nodes.mapped_region...\n"
      indexObject = {"location_nodes.mapped_region"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.mapped_region has been created successfully!\n"
      puts "Creating Index on business_nodes.sales_level_6...\n"
      indexObject = {"location_nodes.sales_level_6"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.sales_level_6 has been created successfully!\n"
      puts "Creating Index on business_nodes.mapped_sales_level_6...\n"
      indexObject = {"location_nodes.mapped_sales_level_6"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.mapped_sales_level_6 has been created successfully!\n"
      puts "Creating Compound Index on business_nodes.gtmu|region...\n"
      indexObject = {"location_nodes.gtmu"  => 1, "location_nodes.region"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Compound Index on business_nodes.gtmu|region has been created successfully!\n"
      puts "Creating Compound Index on business_nodes.gtmu|region|sales_level_6...\n"
      indexObject = {"location_nodes.gtmu"  => 1, "location_nodes.region"  => 1, "location_nodes.sales_level_6"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Compound Index on business_nodes.gtmu|region|sales_level_6 has been created successfully!\n"
      puts "Creating Compound Index on business_nodes.gtmu|region|sales_level_6|names.sales_agent.name...\n"
      indexObject = {"location_nodes.gtmu"  => 1, "location_nodes.region"  => 1, "location_nodes.sales_level_6"  => 1, "names.sales_agent.name"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Compound Index on business_nodes.gtmu|region|sales_level_6|names.sales_agent.name has been created successfully!\n"
      puts "Creating Index on business_nodes.bill_to_site_city...\n"
      indexObject = {"location_nodes.bill_to_site_city"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.bill_to_site_city has been created successfully!\n"
      puts "Creating Index on business_nodes.ship_to_city...\n"
      indexObject = {"location_nodes.ship_to_city"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.ship_to_city has been created successfully!\n"
      changing_end_time = Time.now
      time_elapsed_secs = changing_end_time-changing_starting_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "Elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      time_elapsed_secs = changing_end_time-start_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "So far elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      
      # Creating Index for References
      changing_starting_time = Time.now
      puts "Creating Index on business_nodes.erp_deal_id...\n"
      indexObject = {"references.erp_deal_id"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.erp_deal_id has been created successfully!\n"
      puts "Creating Index on business_nodes.sales_order_number_detail...\n"
      indexObject = {"references.sales_order_number_detail"  => 1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on business_nodes.sales_order_number_detail has been created successfully!\n"
      changing_end_time = Time.now
      time_elapsed_secs = changing_end_time-changing_starting_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "Elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      time_elapsed_secs = changing_end_time-start_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "So far elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      
      # Creating Index for Metrics
=begin
      changing_starting_time = Time.now
      puts "Creating Index on metric.booking_net...\n"
      indexObject = {"metric.booking_net"  => -1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on metric.booking_net has been created successfully!\n"
      puts "Creating Index on metric.base_list...\n"
      indexObject = {"metric.base_list"  => -1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Index on metric.base_list has been created successfully!\n"
      puts "Creating Compound Index on metric.booking_net|region|base_list...\n"
      indexObject = {"metric.booking_net"  => -1, "metric.base_list"  => -1}
      mongoEngine.createIndex(mongoEngine.getCollection(),indexObject)
      puts "Compound Index on metric.booking_net|region|base_list has been created successfully!\n"
      changing_end_time = Time.now
      time_elapsed_secs = changing_end_time-changing_starting_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "Elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      time_elapsed_secs = changing_end_time-start_time
      time_elapsed_mins = (time_elapsed_secs/60).to_i
      time_elapsed_hrs = (time_elapsed_secs/60*60).to_i
      puts "So far elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
      puts "All Index Process has completed!\n"
=end            
    end # End of create Indexes Method
  end # End of IndexHandler class
  
end # End of the Module
