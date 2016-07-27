module GeneralDashboard


    # Namespace, Module and Class Loading
    require './database_handles/Connections'
    require './database_handles/QueryEngine'
    require './modules/truenorth_dalet/DataStructures'
    require './helpers/Constants'
    require './helpers/Calculators'
    #require 'awesome_print'
    include Connections
    include QueryEngine
    include DataStructures
    include Constants
    include ScalarCalculators
    include ArrayCalculators

    class SubsetBookingData
        attr_accessor :queryObj, :mongoObj, :dsObj, \
            :bookingDumpColl, :usersColl, :gdColl, :mongoEng

        def initialize()
            puts "Acquiring MongoDB Object....\n"
            @mongoObj = MongoDBClient.new()
            puts "MongoDB Object acquired!\n"
            
            puts "Acquiring DataStructure Object....\n"
            @dsObj = BookingDumpDS.new()
            puts "DataStructure Object acquired!\n"
            
            puts "Acquiring Mongo Engine Object ...\n"
            mongoEngine = MongoQuery.new(@mongoObj.getMongoClient, 'booking_dump2')
            puts "Mongo Engine Object acquired!\n"
            @bookingDumpColl = mongoEngine.getCollection()
            mongoEngine = MongoQuery.new(@mongoObj.getMongoClient, 'users')
            @usersColl = mongoEngine.getCollection()
            mongoEngine = MongoQuery.new(@mongoObj.getMongoClient, 'general_dashboard')
            @gdColl = mongoEngine.getCollection()
            @mongoEng = mongoEngine
        end


        # Method to Service subset data for YoY and store in separate collection
        def runServiceSubsetYoY()
            start_time = Time.now
            maxYear, maxQuarter, maxMonth, maxWeek = getMaxPeriods()
            prevYear = maxYear.to_i - 1; prevMonth = maxMonth.to_i
            
            # Obtaining Mongo objects
            mongoObj = MongoDBClient.new()
            mongoEngine = MongoQuery.new(mongoObj.getMongoClient, 'general_dashboard_prodser')
            gdColl = mongoEngine.getCollection()

            mongoEngine3 = MongoQuery.new(mongoObj.getMongoClient, 'booking_dump2')
            bookingColl = mongoEngine3.getCollection()

            puts "Removing the old documents from general_dashboard_ser_yoy collection..."
            mongoEngine2 = MongoQuery.new(mongoObj.getMongoClient, 'general_dashboard_ser_yoy')
            yoyColl = mongoEngine2.getCollection()
            mongoEngine2.removeCollection(yoyColl)
            puts "Old documents from general_dashboard_ser_yoy collection has been removed!"

            puts "Getting All approved users"
            users = getAllApprovedUsers()
            puts "All approved users acquired"

            users.each do |user|
                dataDict = {}
                #user = 'jeydurai'
                mainDoc = {
                    :username => user,
                }

                # Preparing data dictionary to pass as arguments
                dataDict = {
                    :user => user,
                    :prodSer => 'service',
                    :coll => gdColl,
                    :maxYear => maxYear,
                    :maxQuarter => maxQuarter,
                    :maxMonth => maxMonth,
                    :maxWeek => maxWeek,
                    :prevYear => prevYear,
                    :prevMonth => prevMonth,
                    :symbol => :tdBooking,
                    :bookingColl => bookingColl,
                }
                mainDoc[:tdBooking] = getYoYTDBooking(dataDict)
                dataDict[:symbol] = :billedCustomers
                dataDict[:byName] = 'customer'
                mainDoc[:billedCustomers] = getYoYBilledCount(dataDict)
                dataDict[:symbol] = :billedPartners
                dataDict[:byName] = 'partner'
                mainDoc[:billedPartners] = getYoYBilledCount(dataDict)
                dataDict.delete("byName")
                dataDict[:symbol] = :techPenetration
                mainDoc[:techPenetration] = getYoYTechPenetration(dataDict)
                dataDict[:symbol] = :yieldPerCustomer
                mainDoc[:yieldPerCustomer] = getYoYYieldPerCustomer(dataDict, mainDoc[:tdBooking], mainDoc[:billedCustomers])
                dataDict[:symbol] = :archBooking
                mainDoc[:archBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :verticalBooking
                mainDoc[:verticalBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :techBooking
                mainDoc[:techBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :atAttachBooking
                mainDoc[:atAttachBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :subSCMSBooking
                mainDoc[:subSCMSBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :gtmuBooking
                mainDoc[:gtmuBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :regionBooking
                mainDoc[:regionBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :prodSerBooking
                mainDoc[:prodSerBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :disArchsBooking
                mainDoc[:disArchsBooking] = getArrayYoYDiscount(dataDict)
                dataDict[:symbol] = :disAllBooking
                mainDoc[:disAllBooking] = getArrayYoYDiscount(dataDict)
                #ap mainDoc
                #puts "Initializing mongo insertion for #{user} - Product"
                id = mongoEngine2.insertDocument(yoyColl, mainDoc) 
                puts "#{user} - insertion ID: #{id}"
            end # End of users iteration
            end_time = Time.now
            time_elapsed_secs = end_time-start_time
            time_elapsed_mins = (time_elapsed_secs/60).to_i
            time_elapsed_hrs = (time_elapsed_secs/60/60).to_i
            puts "elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
        end # End of runSubsetYoY

        # Method to Product subset data for YoY and store in separate collection
        def runProductSubsetYoY()
            start_time = Time.now
            maxYear, maxQuarter, maxMonth, maxWeek = getMaxPeriods()
            prevYear = maxYear.to_i - 1; prevMonth = maxMonth.to_i
            
            # Obtaining Mongo objects
            mongoObj = MongoDBClient.new()
            mongoEngine = MongoQuery.new(mongoObj.getMongoClient, 'general_dashboard_prodser')
            gdColl = mongoEngine.getCollection()

            mongoEngine3 = MongoQuery.new(mongoObj.getMongoClient, 'booking_dump2')
            bookingColl = mongoEngine3.getCollection()

            puts "Removing the old documents from general_dashboard_prod_yoy collection..."
            mongoEngine2 = MongoQuery.new(mongoObj.getMongoClient, 'general_dashboard_prod_yoy')
            yoyColl = mongoEngine2.getCollection()
            mongoEngine2.removeCollection(yoyColl)
            puts "Old documents from general_dashboard_prod_yoy collection has been removed!"

            puts "Getting All approved users"
            users = getAllApprovedUsers()
            puts "All approved users acquired"

            users.each do |user|
                dataDict = {}
                #user = 'jeydurai'
                mainDoc = {
                    :username => user,
                }

                # Preparing data dictionary to pass as arguments
                dataDict = {
                    :user => user,
                    :prodSer => 'product',
                    :coll => gdColl,
                    :maxYear => maxYear,
                    :maxQuarter => maxQuarter,
                    :maxMonth => maxMonth,
                    :maxWeek => maxWeek,
                    :prevYear => prevYear,
                    :prevMonth => prevMonth,
                    :symbol => :tdBooking,
                    :bookingColl => bookingColl,
                }
                mainDoc[:tdBooking] = getYoYTDBooking(dataDict)
                dataDict[:symbol] = :billedCustomers
                dataDict[:byName] = 'customer'
                mainDoc[:billedCustomers] = getYoYBilledCount(dataDict)
                dataDict[:symbol] = :billedPartners
                dataDict[:byName] = 'partner'
                mainDoc[:billedPartners] = getYoYBilledCount(dataDict)
                dataDict.delete("byName")
                dataDict[:symbol] = :techPenetration
                mainDoc[:techPenetration] = getYoYTechPenetration(dataDict)
                dataDict[:symbol] = :yieldPerCustomer
                mainDoc[:yieldPerCustomer] = getYoYYieldPerCustomer(dataDict, mainDoc[:tdBooking], mainDoc[:billedCustomers])
                dataDict[:symbol] = :archBooking
                mainDoc[:archBooking] = getArrayYoYBooking(dataDict)
                #puts mainDoc[:archBooking]
                dataDict[:symbol] = :verticalBooking
                mainDoc[:verticalBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :techBooking
                mainDoc[:techBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :atAttachBooking
                mainDoc[:atAttachBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :subSCMSBooking
                mainDoc[:subSCMSBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :gtmuBooking
                mainDoc[:gtmuBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :regionBooking
                mainDoc[:regionBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :prodSerBooking
                mainDoc[:prodSerBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :disArchsBooking
                mainDoc[:disArchsBooking] = getArrayYoYDiscount(dataDict)
                dataDict[:symbol] = :disAllBooking
                mainDoc[:disAllBooking] = getArrayYoYDiscount(dataDict)
                #ap mainDoc
                #puts "Initializing mongo insertion for #{user} - Product"
                id = mongoEngine2.insertDocument(yoyColl, mainDoc) 
                puts "#{user} - insertion ID: #{id}"
            end # End of users iteration
            end_time = Time.now
            time_elapsed_secs = end_time-start_time
            time_elapsed_mins = (time_elapsed_secs/60).to_i
            time_elapsed_hrs = (time_elapsed_secs/60/60).to_i
            puts "elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
        end # End of runSubsetYoY

        # Method to subset data for YoY and store in separate collection
        def runSubsetYoY()
            start_time = Time.now
            maxYear, maxQuarter, maxMonth, maxWeek = getMaxPeriods()
            prevYear = maxYear.to_i - 1; prevMonth = maxMonth.to_i
            prevQuarter = maxQuarter
            
            # Obtaining Mongo objects
            mongoObj = MongoDBClient.new()
            mongoEngine = MongoQuery.new(mongoObj.getMongoClient, 'general_dashboard')
            gdColl = mongoEngine.getCollection()


            mongoEngine3 = MongoQuery.new(mongoObj.getMongoClient, 'booking_dump2')
            bookingColl = mongoEngine3.getCollection()
            
            puts "Removing the old documents from general_dashboard collection..."
            mongoEngine2 = MongoQuery.new(mongoObj.getMongoClient, 'general_dashboard_yoy')
            yoyColl = mongoEngine2.getCollection()
            mongoEngine2.removeCollection(yoyColl)
            puts "Old documents from general_dashboard_yoy collection has been removed!"

            puts "Getting All approved users"
            users = getAllApprovedUsers()
            puts "All approved users acquired"

            users.each do |user|
                dataDict = {}
                #user = 'jeydurai'
                mainDoc = {
                    :username => user,
                }

                # Preparing data dictionary to pass as arguments
                dataDict = {
                    :user => user,
                    :coll => gdColl,
                    :maxYear => maxYear,
                    :maxQuarter => maxQuarter,
                    :maxMonth => maxMonth,
                    :maxWeek => maxWeek,
                    :prevYear => prevYear,
                    :prevMonth => prevMonth,
                    :prevQuarter => prevQuarter,
                    :symbol => :tdBooking,
                    :bookingColl => bookingColl,
                }
                mainDoc[:tdBooking] = getYoYTDBooking(dataDict)
                dataDict[:symbol] = :billedCustomers
                dataDict[:byName] = 'customer'
                mainDoc[:billedCustomers] = getYoYBilledCount(dataDict)
                dataDict[:symbol] = :billedPartners
                dataDict[:byName] = 'partner'
                mainDoc[:billedPartners] = getYoYBilledCount(dataDict)
                dataDict.delete("byName")
                dataDict[:symbol] = :techPenetration
                mainDoc[:techPenetration] = getYoYTechPenetration(dataDict)
                dataDict[:symbol] = :yieldPerCustomer
                mainDoc[:yieldPerCustomer] = getYoYYieldPerCustomer(dataDict, mainDoc[:tdBooking], mainDoc[:billedCustomers])
                dataDict[:symbol] = :archBooking
                mainDoc[:archBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :verticalBooking
                mainDoc[:verticalBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :techBooking
                mainDoc[:techBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :atAttachBooking
                mainDoc[:atAttachBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :subSCMSBooking
                mainDoc[:subSCMSBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :gtmuBooking
                mainDoc[:gtmuBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :regionBooking
                mainDoc[:regionBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :prodSerBooking
                mainDoc[:prodSerBooking] = getArrayYoYBooking(dataDict)
                dataDict[:symbol] = :disArchsBooking
                mainDoc[:disArchsBooking] = getArrayYoYDiscount(dataDict)
                dataDict[:symbol] = :disAllBooking
                mainDoc[:disAllBooking] = getArrayYoYDiscount(dataDict)
                #ap mainDoc
                #puts "Initializing mongo insertion for #{user}"
                id = mongoEngine2.insertDocument(yoyColl, mainDoc) 
                puts "#{user} - insertion ID: #{id}"
            end # End of users iteration
            end_time = Time.now
            time_elapsed_secs = end_time-start_time
            time_elapsed_mins = (time_elapsed_secs/60).to_i
            time_elapsed_hrs = (time_elapsed_secs/60/60).to_i
            puts "elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"
        end # End of runSubsetYoY

        def getArrayYoYBooking(dataDict)
            #ap dataDict
            #puts "Outside Testing: " + dataDict[:prodSer]

            currentXAxis, tempXAxis = [], []
            currentYAxis, tempYAxis = [], [] 
            if dataDict.has_key? :prodSer
                #puts "Testing: " + dataDict[:prodSer]
                queryObj = {
                        :username => dataDict[:user],
                        "periods.prod_ser" => dataDict[:prodSer],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            else
                #puts "Testing: failed" 
                queryObj = {
                        :username => dataDict[:user],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            end # End of If condition to check if the key prodSer is there
            #ap queryObj
            currentDoc = dataDict[:coll].find(queryObj)
            currentDoc.each do |doc|
                currentXAxis = doc[dataDict[:symbol]][:xAxis]
                currentYAxis = doc[dataDict[:symbol]][:yAxis]
            end # End of CurrentDoc iteration

            prevXAxis, prevYAxis = sumParallelArray(currentXAxis, dataDict)

            currentDict = {
                :xAxis => currentXAxis,
                :yAxis => currentYAxis
            }
            prevDict = {
                :xAxis => prevXAxis,
                :yAxis => prevYAxis
            }
            return ArrayCalculators.calculateGrowth(currentDict, prevDict)
        end # End of getArrayYoYBooking method

        def getArrayYoYDiscount(dataDict)

            currentXAxis, tempXAxis = [], []
            currentYAxis, tempYAxis = [], [] 
            currentYAxis2, tempYAxis2 = [], [] 
            currentYAxis3, tempYAxis3 = [], [] 
            if dataDict.has_key? :prodSer
                queryObj = {
                        :username => dataDict[:user],
                        "periods.prod_ser" => dataDict[:prodSer],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            else
                queryObj = {
                        :username => dataDict[:user],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            end # End of If condition to check if the key prodSer is there
            currentDoc = dataDict[:coll].find(queryObj)
            currentDoc.each do |doc|
                currentXAxis = doc[dataDict[:symbol]][:xAxis]
                currentYAxis = doc[dataDict[:symbol]][:yAxis]
                currentYAxis2 = doc[dataDict[:symbol]][:yAxis2]
                currentYAxis3 = doc[dataDict[:symbol]][:yAxis3]
            end # End of CurrentDoc iteration

            prevXAxis, prevYAxis = sumParallelArrayDiscount(currentXAxis, dataDict)

            currentDict = {
                :xAxis => currentXAxis,
                :yAxis => currentYAxis
            }
            prevDict = {
                :xAxis => prevXAxis,
                :yAxis => prevYAxis
            }
            return ArrayCalculators.calculateGrowth(currentDict, prevDict)
        end # End of getArrayYoYDiscount method

        def getYoYTDBooking(dataDict)
            current, prev = 0.0, 0.0

            if dataDict.has_key? :prodSer
                queryObj = {
                        :username => dataDict[:user],
                        "periods.prod_ser" => dataDict[:prodSer],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            else
                queryObj = {
                        :username => dataDict[:user],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            end # End of If condition to check if the key prodSer is there

            currentDoc = dataDict[:coll].find(queryObj)
            currentDoc.each do |doc|
                current = doc[dataDict[:symbol]][:yAxis][0]
            end
            tempTotal = 0.0
            dataDict[:prevMonth].times do |i|
                month = i + 1
                if dataDict.has_key? :prodSer
                    queryObj2 = {
                            :username => dataDict[:user],
                            "periods.prod_ser" => dataDict[:prodSer],
                            "periods.year" => dataDict[:prevYear].to_s,
                            "periods.month" => month.to_s,
                            "periods.week" => nil,
                    }
                else
                    queryObj2 = {
                            :username => dataDict[:user],
                            "periods.year" => dataDict[:prevYear].to_s,
                            "periods.month" => month.to_s,
                            "periods.week" => nil,
                    }
                end # End of If condition to check if the key prodSer is there
                prevDoc = dataDict[:coll].find(queryObj2)
                prevDoc.each do |doc|
                    tempTotal = doc[:tdBooking][:yAxis][0]
                end # End of prevYearDoc iteration
                prev += tempTotal
            end # End of Month iteration
            returnData = {
                :xAxis => [dataDict[:maxYear]],
                :yAxis => [ScalarCalculators.calculateGrowth(current, prev)],
                :current => [current],
                :prev => [prev],
            }
            return returnData
        end # End of getYoYTDBooking method


        def getPreviousYearMatchObj(dataDict)
            if dataDict.has_key? :prodSer
                matchObj = {
                    :$match => {
                        :mappedTo => dataDict[:user],
                        :prod_ser => dataDict[:prodSer],
                        "periods.year" => dataDict[:prevYear].to_s,
                    }
                }
            else
                matchObj = {
                    :$match => {
                        :mappedTo => dataDict[:user],
                        "periods.year" => dataDict[:prevYear].to_s,
                    }
                }
            end

            if dataDict[:prevMonth] == 3 || dataDict[:prevMonth] == 6 || 
               dataDict[:prevMonth]  == 9 || dataDict[:prevMonth] == 12
                if dataDict[:prevQuarter] == "Q1"
                    matchObj[:$match]["periods.quarter"] = dataDict[:prevQuarter]
                elsif dataDict[:prevQuarter] == "Q2"
                    matchObj[:$match][:$or] = [
                        { "periods.quarter" => "Q1" },
                        { "periods.quarter" => "Q2" },
                    ]
                elsif dataDict[:prevQuarter] == "Q3"
                    matchObj[:$match][:$or] = [
                        { "periods.quarter" => "Q1" },
                        { "periods.quarter" => "Q2" },
                        { "periods.quarter" => "Q3" },
                    ]
                elsif dataDict[:prevQuarter] == "Q4"
                    matchObj[:$match][:$or] = [
                        { "periods.quarter" => "Q1" },
                        { "periods.quarter" => "Q2" },
                        { "periods.quarter" => "Q3" },
                        { "periods.quarter" => "Q4" },
                    ]
                end
            else
                if dataDict[:prevMonth] < 3
                    matchObj[:$match][:$or] = [
                        { "periods.month" => "1" },
                        { "periods.month" => "2" },
                    ]
                elsif dataDict[:prevMonth] > 3 || dataDict[:prevMonth] < 6
                    matchObj[:$match][:$or] = [
                        { "periods.quarter" => "Q1" },
                        { "periods.month" => "4" },
                        { "periods.month" => "5" },
                    ]
                elsif dataDict[:prevMonth] > 6 || dataDict[:prevMonth] < 9 
                    matchObj[:$match][:$or] = [
                        { "periods.quarter" => "Q1" },
                        { "periods.quarter" => "Q2" },
                        { "periods.month" => "7" },
                        { "periods.month" => "8" },
                    ]
                elsif dataDict[:prevMonth] > 9 || dataDict[:prevMonth] < 12
                    matchObj[:$match][:$or] = [
                        { "periods.quarter" => "Q1" },
                        { "periods.quarter" => "Q2" },
                        { "periods.quarter" => "Q3" },
                        { "periods.month" => "10" },
                        { "periods.month" => "11" },
                    ]
                end
            end # End of If condition to check if the month is end of quarter
            return matchObj
        end # End of getPreviousYearMatchObj method



        # Calculate YoY for Yield per Customer
        def getYoYYieldPerCustomer(dataDict, bookingObj, cusObj)
            current, prev = 0.0, 0.0

            if dataDict.has_key? :prodSer
                queryObj = {
                        :username => dataDict[:user],
                        "periods.prod_ser" => dataDict[:prodSer],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            else
                queryObj = {
                        :username => dataDict[:user],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            end # End of If condition to check if the key prodSer is there

            currentDoc = dataDict[:coll].find(queryObj)
            currentDoc.each do |doc|
                current = doc[dataDict[:symbol]][:yAxis][0]
            end # End of currentDoc iteration
            prev = ScalarCalculators.calculateRatio(bookingObj[:prev][0]*THOUSAND, cusObj[:prev][0])
            puts "Yield of #{bookingObj[:prev][0]*THOUSAND} per #{cusObj[:prev][0]} customers is #{prev}"
            returnData = {
                :xAxis => [dataDict[:maxYear]],
                :yAxis => [ScalarCalculators.calculateGrowth(current, prev)],
                :current => [current],
                :prev => [prev],
            }
            return returnData
        end # End of getYoYTechPenetration method

        # Calculate YoY for Technology Penetration
        def getYoYTechPenetration(dataDict)
            current, prev = 0.0, 0.0

            if dataDict.has_key? :prodSer
                queryObj = {
                        :username => dataDict[:user],
                        "periods.prod_ser" => dataDict[:prodSer],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
                
            else
                queryObj = {
                        :username => dataDict[:user],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            end # End of If condition to check if the key prodSer is there

            currentDoc = dataDict[:coll].find(queryObj)
            currentDoc.each do |doc|
                current = doc[dataDict[:symbol]][:yAxis][0]
            end # End of currentDoc iteration

            matchObj = getPreviousYearMatchObj(dataDict)
            # ap matchObj
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            # ap matchObj
            aggregateQuery = [
                matchObj,
                @dsObj.groupBookingByCustomerTechnologies(),
            ]
            doc = {}
            if !matchObj.nil?
                returnedObj = keyValuePairByAggQryTechPenetration(aggregateQuery, dataDict[:bookingColl])
                doc[dataDict[:symbol]] = {
                    :xAxis => returnedObj[:key],
                    :yAxis => returnedObj[:value],
                }
            else
                doc[dataDict[:symbol]] = {
                    :xAxis => [],
                    :yAxis => [],
                } 
            end # End of If condition
            if !doc[dataDict[:symbol]][:xAxis].empty?
                prev = doc[dataDict[:symbol]][:yAxis][0]
            end
            returnData = {
                :xAxis => [dataDict[:maxYear]],
                :yAxis => [ScalarCalculators.calculateGrowth(current, prev)],
                :current => [current],
                :prev => [prev],
            }
            return returnData
        end # End of getYoYTechPenetration method

        # Calculate YoY for Billed Customers/Partners
        def getYoYBilledCount(dataDict)
            current, prev = 0.0, 0.0

            if dataDict.has_key? :prodSer
                queryObj = {
                        :username => dataDict[:user],
                        "periods.prod_ser" => dataDict[:prodSer],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            else
                queryObj = {
                        :username => dataDict[:user],
                        "periods.year" => dataDict[:maxYear],
                        "periods.quarter" => nil,
                        "periods.month" => nil,
                        "periods.week" => nil,
                }
            end # End of If condition to check if the key prodSer is there

            currentDoc = dataDict[:coll].find(queryObj)
            currentDoc.each do |doc|
                current = doc[dataDict[:symbol]][:yAxis][0]
            end # End of currentDoc iteration
            matchObj = getPreviousYearMatchObj(dataDict)
            if dataDict[:byName] == 'customer'
                matchObj[:$match][:$and] = [
                    {
                        "names.customer.unique_name" => {
                            :$not => /^unknown/i
                        }
                    },
                    {
                        "names.customer.unique_name" => {
                            :$not => /^small busi/i
                        }
                    },
                    {
                        "names.customer.unique_name" => {
                            :$not => /^cobo una/i
                        }
                    },
                    {
                        "names.customer.unique_name" => {
                            :$not => /^run rate/i
                        }
                    },
                    {
                        "names.customer.unique_name" => {
                            :$not => /^runrate/i
                        }
                    }
                ] 
                aggregateQuery = [
                    matchObj,
                    @dsObj.groupByCustomers(),
                ]
            else
                matchObj[:$match][:$and] = [
                    {
                        "names.partner.unique_name" => {
                            :$not => /^unknown/i
                        }
                    },
                    {
                        "names.partner.unique_name" => {
                            :$not => /^small busi/i
                        }
                    },
                    {
                        "names.partner.unique_name" => {
                            :$not => /^cobo una/i
                        }
                    },
                    {
                        "names.partner.unique_name" => {
                            :$not => /^run rate/i
                        }
                    },
                    {
                        "names.partner.unique_name" => {
                            :$not => /^runrate/i
                        }
                    }
                ] 
                aggregateQuery = [
                    matchObj,
                    @dsObj.groupByPartners(),
                ]
            end # End of If condition
            doc = {}
            if !matchObj.nil?
                returnedObj = keyValuePairByAggQryBilled(aggregateQuery, dataDict[:bookingColl], dataDict[:byName])
                doc[dataDict[:symbol]] = {
                    :xAxis => returnedObj[:key],
                    :yAxis => returnedObj[:value]
                }
            else
                doc[dataDict[:symbol]] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition
            if !doc[dataDict[:symbol]][:xAxis].empty?
                prev = doc[dataDict[:symbol]][:yAxis][0]
            end
            returnData = {
                :xAxis => [dataDict[:maxYear]],
                :yAxis => [ScalarCalculators.calculateGrowth(current, prev)],
                :current => [current],
                :prev => [prev],
            }
            return returnData
        end # End of getYoYBilledCount method

        # Main method to run the the subset data for general dashboard
        # Product/Service
        def runProdSerSubset()

            start_time = Time.now

            puts "Removing the old documents from general_dashboard_prodser collection..."
            mongoObj = MongoDBClient.new()
            dsObj = BookingDumpDS.new()

            mongoEngine = MongoQuery.new(mongoObj.getMongoClient, 'booking_dump2')
            bookingColl = mongoEngine.getCollection()

            mongoEngine2 = MongoQuery.new(mongoObj.getMongoClient, 'general_dashboard_prodser')
            gdColl = mongoEngine2.getCollection()
            mongoEngine2.removeCollection(gdColl)
            mongoEng = mongoEngine2
            puts "Old documents from general_dashboard_prodser collection has been removed!"

            maxYear, maxQuarter, maxMonth, maxWeek = getMaxPeriods()
            puts "Getting all periods"
            allPeriods = getAllPeriods()
            puts "All Periods acquired from database"
            puts "Getting All approved users"
            users = getAllApprovedUsers()
            puts "All approved users acquired"


            periodsArray = allPeriods[:pArray]
            dataDict = {}
            combineArray = []
            prodSerArray = ['product', 'service'];
            prodSerArray.each do |prodSer|
                users.each do |user|
                    periodsArray.each do |periodDict|
                        start_time_inner = Time.now
                        year = periodDict[:periods][:year]
                        quarter = periodDict[:periods][:quarter]
                        month = periodDict[:periods][:month]
                        week = periodDict[:periods][:week]
                        if !combineArray.include? user+'|'+year.to_s+'|'+prodSer
                            dataDict = {
                                :userName => user,
                                :fiscalYear => year,
                                :fiscalQuarter => nil,
                                :fiscalMonth => nil,
                                :fiscalWeek => nil,
                                :prodSer => prodSer,
                                :bookingColl => bookingColl,
                                :gdColl =>gdColl,
                                :mongoEng => mongoEng,
                                :dsObj => dsObj,
                            }
                            generateProdSerDataset(dataDict)
                        end
                        if !combineArray.include? user+'|'+year.to_s+'|'+quarter+'|'+prodSer
                            dataDict = {
                                :userName => user,
                                :fiscalYear => year,
                                :fiscalQuarter => quarter,
                                :fiscalMonth => nil,
                                :fiscalWeek => nil,
                                :prodSer => prodSer,
                                :bookingColl => bookingColl,
                                :gdColl =>gdColl,
                                :mongoEng => mongoEng,
                                :dsObj => dsObj,
                            }
                            generateProdSerDataset(dataDict)
                        end
                        if !combineArray.include? user+'|'+year.to_s+'|'+quarter+'|'+month.to_s+'|'+prodSer
                            dataDict = {
                                :userName => user,
                                :fiscalYear => year,
                                :fiscalQuarter => quarter,
                                :fiscalMonth => month,
                                :fiscalWeek => nil,
                                :prodSer => prodSer,
                                :bookingColl => bookingColl,
                                :gdColl =>gdColl,
                                :mongoEng => mongoEng,
                                :dsObj => dsObj,
                            }
                            generateProdSerDataset(dataDict)
                        end
                        if !combineArray.include? user+'|'+year.to_s+'|'+quarter+'|'+month.to_s+'|'+week.to_s+'|'+prodSer 
                            dataDict = {
                                :userName => user,
                                :fiscalYear => year,
                                :fiscalQuarter => quarter,
                                :fiscalMonth => month,
                                :fiscalWeek => week,
                                :prodSer => prodSer,
                                :bookingColl => bookingColl,
                                :gdColl =>gdColl,
                                :mongoEng => mongoEng,
                                :dsObj => dsObj,
                            }
                            generateProdSerDataset(dataDict)
                        end
                        combineArray << user+'|'+year.to_s+'|'+prodSer; combineArray << user+'|'+year.to_s+'|'+quarter+'|'+prodSer; combineArray << user+'|'+year.to_s+'|'+quarter+'|'+month.to_s+'|'+prodSer;
                        combineArray << user+'|'+year.to_s+'|'+quarter+'|'+month.to_s+'|'+week.to_s+'|'+prodSer
                        combineArray = combineArray.uniq
                        end_time_inner = Time.now
                        time_elapsed_secs_inner = end_time_inner-start_time_inner
                        time_elapsed_mins_inner = (time_elapsed_secs_inner/60)
                        time_elapsed_hrs_inner = (time_elapsed_secs_inner/60/60)
                        puts "#{user+'|'+year.to_s+'|'+quarter+'|'+month.to_s+'|'+week.to_s} elapsed #{time_elapsed_secs_inner.round(2)} sec(s) | #{time_elapsed_mins_inner.round(2)} min(s) | #{time_elapsed_hrs_inner.round(2)} hr(s)\n"
                    end # End of periodsArray iteration
                end # End of users iteration
            end # End of prodSerArray iteration
            end_time = Time.now
            time_elapsed_secs = end_time-start_time
            time_elapsed_mins = (time_elapsed_secs/60).to_i
            time_elapsed_hrs = (time_elapsed_secs/60/60).to_i
            puts "elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins} min(s) | #{time_elapsed_hrs} hr(s)\n"

        end # End of runSubset method
        # /////////////////////////////////////////////////////////////
        # =============================================================



        # Main method to run the the subset data for general dashboard
        def runSubset()

            start_time = Time.now

            puts "Removing the old documents from general_dashboard collection..."
            mongoObj = MongoDBClient.new()
            dsObj = BookingDumpDS.new()

            mongoEngine = MongoQuery.new(mongoObj.getMongoClient, 'booking_dump2')
            bookingColl = mongoEngine.getCollection()
            mongoEngine2 = MongoQuery.new(mongoObj.getMongoClient, 'general_dashboard')
            gdColl = mongoEngine2.getCollection()
            mongoEngine2.removeCollection(gdColl)
            mongoEng = mongoEngine2
            puts "Old documents from general_dashboard collection has been removed!"

            maxYear, maxQuarter, maxMonth, maxWeek = getMaxPeriods()
            puts "Getting all periods"
            allPeriods = getAllPeriods()
            puts "All Periods acquired from database"
            puts "Getting All approved users"
            users = getAllApprovedUsers()
            puts "All approved users acquired"


            periodsArray = allPeriods[:pArray]
            dataDict = {}
            combineArray = []
            users.each do |user|
                start_time_inner = Time.now
                periodsArray.each do |periodDict|
                    year = periodDict[:periods][:year]
                    quarter = periodDict[:periods][:quarter]
                    month = periodDict[:periods][:month]
                    week = periodDict[:periods][:week]
                    if !combineArray.include? user+'|'+year.to_s
                        dataDict = {
                            :userName => user,
                            :fiscalYear => year,
                            :fiscalQuarter => nil,
                            :fiscalMonth => nil,
                            :fiscalWeek => nil,
                            :bookingColl => bookingColl,
                            :gdColl =>gdColl,
                            :mongoEng => mongoEng,
                            :dsObj => dsObj,
                        }
                        generateDataset(dataDict)
                    end
                    if !combineArray.include? user+'|'+year.to_s+'|'+quarter 
                        dataDict = {
                            :userName => user,
                            :fiscalYear => year,
                            :fiscalQuarter => quarter,
                            :fiscalMonth => nil,
                            :fiscalWeek => nil,
                            :bookingColl => bookingColl,
                            :gdColl =>gdColl,
                            :mongoEng => mongoEng,
                            :dsObj => dsObj,
                        }
                        generateDataset(dataDict)
                    end
                    if !combineArray.include? user+'|'+year.to_s+'|'+quarter+'|'+month.to_s 
                        dataDict = {
                            :userName => user,
                            :fiscalYear => year,
                            :fiscalQuarter => quarter,
                            :fiscalMonth => month,
                            :fiscalWeek => nil,
                            :bookingColl => bookingColl,
                            :gdColl =>gdColl,
                            :mongoEng => mongoEng,
                            :dsObj => dsObj,
                        }
                        generateDataset(dataDict)
                    end
                    if !combineArray.include? user+'|'+year.to_s+'|'+quarter+'|'+month.to_s+'|'+week.to_s
                        dataDict = {
                            :userName => user,
                            :fiscalYear => year,
                            :fiscalQuarter => quarter,
                            :fiscalMonth => month,
                            :fiscalWeek => week,
                            :bookingColl => bookingColl,
                            :gdColl =>gdColl,
                            :mongoEng => mongoEng,
                            :dsObj => dsObj,
                        }
                        generateDataset(dataDict)
                    end
                    combineArray << user+'|'+year.to_s; combineArray << user+'|'+year.to_s+'|'+quarter; combineArray << user+'|'+year.to_s+'|'+quarter+'|'+month.to_s;
                    combineArray << user+'|'+year.to_s+'|'+quarter+'|'+month.to_s+'|'+week.to_s
                    combineArray = combineArray.uniq
                    end_time_inner = Time.now
                    time_elapsed_secs_inner = end_time_inner-start_time_inner
                    time_elapsed_mins_inner = (time_elapsed_secs_inner/60)
                    time_elapsed_hrs_inner = (time_elapsed_secs_inner/60/60)
                    puts "#{user+'|'+year.to_s+'|'+quarter+'|'+month.to_s+'|'+week.to_s} elapsed #{time_elapsed_secs_inner.round(2)} sec(s) | #{time_elapsed_mins_inner.round(2)} min(s) | #{time_elapsed_hrs_inner.round(2)} hr(s)\n"
                end # End of periodsArray iteration
            end # End of users iteration
            end_time = Time.now
            time_elapsed_secs = end_time-start_time
            time_elapsed_mins = (time_elapsed_secs/60)
            time_elapsed_hrs = (time_elapsed_secs/60/60)
            puts "elapsed #{time_elapsed_secs.round(2)} sec(s) | #{time_elapsed_mins.round(2)} min(s) | #{time_elapsed_hrs.round(2)} hr(s)\n"

        end # End of runSubset method

        def getAllApprovedUsers()
            users = [] 
            docs = @usersColl.find({ "approval_status.code" => 1 }, { :username => 1 })
            index = 0
            docs.each do |doc|
                users[index] = doc[:username]
                index += 1
            end # End of docs iteration
            return users
        end # End of getAllApprovedUsers method

        def getAllPeriods()
            aggregateQuery = [
                @dsObj.groupByPeriods(),
                {:$sort => {"_id.fiscal_year" => -1, "_id.fiscal_quarter" => -1, "_id.fiscal_month" => -1, "_id.fiscal_week" => -1}}
            ]
            aggregateCursor = @bookingDumpColl.aggregate(aggregateQuery)

            yearArray = []; quarterArray = []; monthArray = []; weekArray = [];
            periodsArray = [] 
            periodDict = {}

            aggregateCursor.each do |doc|
                subDoc = doc[:_id]
                yearArray << subDoc[:fiscal_year]
                quarterArray << subDoc[:fiscal_quarter]
                monthArray << subDoc[:fiscal_month]
                weekArray << subDoc[:fiscal_week]
                periodDict = {
                    :periods => {
                        :year => subDoc[:fiscal_year],
                        :quarter => subDoc[:fiscal_quarter],
                        :month => subDoc[:fiscal_month],
                        :week => subDoc[:fiscal_week]
                    }
                }
                periodsArray << periodDict
            end
            yearArray = yearArray.uniq; yearArray = yearArray.sort
            quarterArray = quarterArray.uniq; quarterArray = quarterArray.sort
            monthArray = monthArray.uniq; monthArray = monthArray.sort
            weekArray = weekArray.uniq; weekArray = weekArray.sort

            arrayHash = {
                :yArray => yearArray,
                :qArray => quarterArray,
                :mArray => monthArray,
                :wArray => weekArray,
                :pArray => periodsArray
            }
            return arrayHash
            
        end # End of getAllPeriods method


        # Aggregate Query method Billed Customers/Partners
        def keyValuePairByAggQryBilled(aggregateQuery, bookingColl, byName)
            aggregateCursor = bookingColl.aggregate(aggregateQuery)
            keyArray = []; valueArray = []; returnObj = {}
            uniqueArray = [];

            aggregateCursor.each do |doc|
                uniqueArray << doc[:_id]
            end
            keyArray << byName; valueArray << uniqueArray.size 
            returnObj = {
                :key => keyArray,
                :value => valueArray
            }
            return returnObj
        end # End of method keyValuePairByAggQryBilled


        # Aggregate Query method Billed Customers (Repeat / New)
        def keyValuePairByAggQryRepeatNew(aggregateQueries, bookingColl, byName)
            aggregateQuery1 = aggregateQueries[:query1]
            aggregateQuery2 = aggregateQueries[:query2]
            aggregateQuery3 = aggregateQueries[:query3]

            aggregateCursor1 = bookingColl.aggregate(aggregateQuery1)
            aggregateCursor2 = bookingColl.aggregate(aggregateQuery2)
            aggregateCursor3 = bookingColl.aggregate(aggregateQuery3)
            keyArray = []; returnObj1 = {}; returnObj2 = {}; returnObj3 = {}
            uniqueArray1 = []; uniqueArray2 = []; uniqueArray3 = []
            bookingArray1 = []; bookingArray2 = []; bookingArray3 = [] 
            newBookingToPrevArray = []; newBookingToMorePrevArray = [] 
            dormantBookingToPrevArray = []; dormantBookingToMorePrevArray = []
            uniqueDict1 = {}; uniqueDict2 = {}; uniqueDict3 = {}

            aggregateCursor1.each do |doc|
                uniqueArray1 << doc[:_id]
                uniqueDict1[doc[:_id]] = doc[:booking]
            end
            aggregateCursor2.each do |doc|
                uniqueArray2 << doc[:_id]
                uniqueDict2[doc[:_id]] = doc[:booking]
            end
            aggregateCursor3.each do |doc|
                uniqueArray3 << doc[:_id]
                uniqueDict3[doc[:_id]] = doc[:booking]
            end

            newToPrevArray = uniqueArray1 - uniqueArray2
            newBookingToPrev = 0.0
            newToPrevArray.each do |element|
                if !uniqueDict1[element].nil?
                    newBookingToPrev += uniqueDict1[element]
                end
            end
            #puts "New Booking to Prev: #{newBookingToPrev}"

            newToMorePrevArray = uniqueArray1 - uniqueArray3
            newBookingToMorePrev = 0.0
            newToMorePrevArray.each do |element|
                if !uniqueDict1[element].nil?
                    newBookingToMorePrev += uniqueDict1[element]
                end
            end
            #puts "New Booking to More Prev: #{newBookingToMorePrev}"

            dormantToPrevArray = uniqueArray2 - uniqueArray1
            dormantBookingToPrev = 0.0
            dormantToPrevArray.each do |element|
                if !uniqueDict2[element].nil?
                    dormantBookingToPrev += uniqueDict2[element]
                end
            end
            #puts "Dormant Booking to Prev: #{dormantBookingToPrev}"
            
            dormantToMorePrevArray = uniqueArray3 - uniqueArray1
            dormantBookingToMorePrev = 0.0
            dormantToMorePrevArray.each do |element|
                if !uniqueDict3[element].nil?
                    dormantBookingToMorePrev += uniqueDict3[element]
                end
            end
            #puts "Dormant Booking to More Prev: #{dormantBookingToMorePrev}"

            totalAccounts = uniqueArray1.size # Total Number of accounts
            totalAccountsPrev = uniqueArray2.size # Total Last Year Accounts
            totalAccountsMorePrev = uniqueArray3.size # Total Last 3 Year Accounts

            totalBooking1 = 0.0
            uniqueDict1.each do |cust, booking|
                totalBooking1 += booking
            end

            totalBooking2 = 0.0
            uniqueDict2.each do |cust, booking|
                totalBooking2 += booking
            end

            totalBooking3 = 0.0
            uniqueDict3.each do |cust, booking|
                totalBooking3 += booking
            end

            newAccountsToPrev = newToPrevArray.size # New accounts compared to Last year
            newAccountsToMorePrev = newToMorePrevArray.size # New Accounts compared to last 3 years

            # Calculate the counts and total booking from the array
            repeatAccountsToPrev = totalAccounts - newAccountsToPrev # Repeat accounts compared to last year
            repeatBookingToPrev = totalBooking1 - newBookingToPrev # Repeat accounts' booking compared to last year

            repeatAccountsToMorePrev = totalAccounts - newAccountsToMorePrev # Repeat accounts compared to last 3 years
            repeatBookingToMorePrev = totalBooking1 - newBookingToMorePrev # Repeat accounts' booking compared to last 3 year
            
            dormantAccountsToPrev = dormantToPrevArray.size # Dormant accounts compared to last year

            dormantAccountsToMorePrev = dormantToMorePrevArray.size # Dormant accounts compared to last 3 years


            keyArray << byName 
            returnObj1 = {
                :key => keyArray,
                :val => totalAccounts,
                :value => totalBooking1/THOUSAND,
                :value2 => newAccountsToPrev,
                :value3 => newBookingToPrev/THOUSAND,
                :value4 => repeatAccountsToPrev,
                :value5 => repeatBookingToPrev/THOUSAND,
            }
            returnObj2 = {
                :key => keyArray,
                :val => totalAccounts,
                :value => totalBooking2/THOUSAND,
                :value2 => newAccountsToMorePrev,
                :value3 => newBookingToMorePrev/THOUSAND,
                :value4 => repeatAccountsToMorePrev,
                :value5 => repeatBookingToMorePrev/THOUSAND,
            }
            returnObj3 = {
                :key => keyArray,
                :val => totalAccountsPrev,
                :value => totalBooking3/THOUSAND,
                :value2 => dormantAccountsToPrev,
                :value3 => dormantBookingToPrev/THOUSAND,
                :value4 => totalAccountsMorePrev,
                :value5 => dormantAccountsToMorePrev,
                :value6 => dormantBookingToMorePrev/THOUSAND,
            }
            returnObj = {
                :obj1 => returnObj1,
                :obj2 => returnObj2,
                :obj3 => returnObj3,
            }
            return returnObj
        end # End of method keyValuePairByAggQryRepeatNew

        # Aggregate Query method - Top Deals
        def keyValuePairByAggQryTopDeals(aggregateQuery, bookingColl)
            aggregateCursor = bookingColl.aggregate(aggregateQuery)
            keyArray = []; valueArray = []; returnObj = {}
            aggregateCursor.each do |doc|
                keyArray << (doc[:_id][:customers] + " (" + doc[:_id][:soNumbers] + ")") ; valueArray << (doc[:booking]/MILLION)
            end
            returnObj = {
                :key => keyArray,
                :value => valueArray
            }
            return returnObj
        end # End of method keyValuePairByAggQryTopDeals


        # Aggregate Query method - Technology Penetration
        def keyValuePairByAggQryTechPenetration(aggregateQuery, bookingColl)
            aggregateCursor = bookingColl.aggregate(aggregateQuery)
            keyArray = []; valueArray = []; returnObj = {}
            uniqueArray = []; techCount = 0; booking = 0
            aggregateCursor.each do |doc|
                techCount += 1
                uniqueArray << doc[:_id][:customers]
            end
            uniqueArray = uniqueArray.uniq
            keyArray << 'techPenetration'; valueArray << ScalarCalculators.calculateRatio(techCount, uniqueArray.size).to_f

            returnObj = {
                :key => keyArray,
                :value => valueArray
            }
            return returnObj
        end # End of method keyValuePairByAggQryTopDeals

        # Aggregate Query method Simple
        def keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
            aggregateCursor = bookingColl.aggregate(aggregateQuery)
            keyArray = []; valueArray = []; returnObj = {}
            aggregateCursor.each do |doc|
                keyArray << doc[:_id]; valueArray << (doc[:booking]/MILLION)
            end
            returnObj = {
                :key => keyArray,
                :value => valueArray
            }
            return returnObj
        end # End of method keyValuePairByAggregateQuery
        
        # Aggregate Query method for Discount
        def keyValuePairByAggQryDiscount(aggregateQuery, bookingColl)
            aggregateCursor = bookingColl.aggregate(aggregateQuery)
            keyArray = []; valueArray = []; valueArray2 = []; valueArray3 = []; returnObj = {}
            aggregateCursor.each do |doc|
                keyArray << doc[:_id]; valueArray << (calculateDiscount(doc[:booking], doc[:base_list]))
                valueArray2 << doc[:booking]; valueArray3 << doc[:base_list];
            end
            returnObj = {
                :key => keyArray,
                :value => valueArray,
                :value2 => valueArray2,
                :value3 => valueArray3,
            }
            return returnObj
        end # End of method keyValuePairByAggregateQuery

        def getMaxPeriods()
            maxYear = ""
            maxQuarter = ""
            maxMonth = ""
            maxWeek = ""

            aggregateQuery = [
               @dsObj.groupMaxYear()
            ]
            aggregateCursor = @bookingDumpColl.aggregate(aggregateQuery)
            aggregateCursor.each do |doc|
                maxYear = doc[:period]
            end
            
            #aggregateQuery = nil
            puts @dsObj.matchByYear(maxYear)
            aggregateQuery = [
                @dsObj.matchByYear(maxYear),
                @dsObj.groupMaxQuarter()
            ]
            aggregateCursor = @bookingDumpColl.aggregate(aggregateQuery)
            aggregateCursor.each do |doc|
                maxQuarter = doc[:period]
            end

            aggregateQuery = nil
            aggregateQuery = [
                @dsObj.matchByYearQuarter(maxYear, maxQuarter),
                @dsObj.groupMaxMonth()
            ]
            aggregateCursor = @bookingDumpColl.aggregate(aggregateQuery)
            aggregateCursor.each do |doc|
                maxMonth = doc[:period]
            end
            
            aggregateQuery = nil
            aggregateQuery = [
                @dsObj.matchByYearQuarterMonth(maxYear, maxQuarter, maxMonth),
                @dsObj.groupMaxWeek()
            ]
            aggregateCursor = @bookingDumpColl.aggregate(aggregateQuery)
            aggregateCursor.each do |doc|
                maxWeek = doc[:period]
            end
            return maxYear, maxQuarter, maxMonth, maxWeek
        end # End of getMaxPeriods method



        # Method to generate data set for general dashboard Product/Service
        def generateProdSerDataset(dataDict)
            user_name = dataDict[:userName]
            fiscal_year = dataDict[:fiscalYear]; fiscal_quarter = dataDict[:fiscalQuarter]
            fiscal_month = dataDict[:fiscalMonth]; fiscal_week = dataDict[:fiscalWeek]; prod_ser = dataDict[:prodSer]
            bookingColl = dataDict[:bookingColl]; gdColl = dataDict[:gdColl];
            mongoEng = dataDict[:mongoEng]; dsObj = dataDict[:dsObj]
            doc = {
                :username => user_name,
                :periods => {
                    :year => fiscal_year,
                    :quarter => fiscal_quarter,
                    :month => fiscal_month,
                    :week => fiscal_week,
                    :prod_ser => prod_ser,
                }
            }
            # Till date Booking
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByHistory(),
                { :$sort => {:_id => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:tdBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                } 
            else
                doc[:tdBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Architecture wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByArch2(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:archBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:archBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Vertical wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByVertical(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:verticalBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:verticalBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Technology wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByTechName(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:techBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:techBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # AT Attach wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByAtAttach(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:atAttachBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:atAttachBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Sub SCMS wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingBySubSCMS(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:subSCMSBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:subSCMSBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # GTMu wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByGTMu(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:gtmuBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:gtmuBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Region wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByRegion(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:regionBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:regionBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Top 10 Customers Ranking by Booking
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 

            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByCustomer(),
                { :$sort => {:booking => -1}},
                { :$limit => 10 }
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:topCustomerBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:topCustomerBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Top 10 Partner Ranking by Booking
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            matchObj[:$match][:$and] = [
                {
                    "names.partner.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByPartner(),
                { :$sort => {:booking => -1}},
                { :$limit => 10 }
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:topPartnerBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:topPartnerBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Top 10 Sales_Level_6 Ranking by Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingBySL6(),
                { :$sort => {:booking => -1}},
                { :$limit => 10 }
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:topSL6Booking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:topSL6Booking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Top 10 Deals
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                },
                {
                    "references.sales_order_number_detail" => {
                        :$ne => nil 
                    }
                },
                {
                    "references.sales_order_number_detail" => {
                        :$ne => "" 
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByCustomerSONumber(),
                { :$sort => {:booking => -1}},
                { :$limit => 10 }
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggQryTopDeals(aggregateQuery, bookingColl)
                doc[:topDeals] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:topDeals] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Billed Customers
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupByCustomers(),
            ]
            if !matchObj.nil?
                returnedObj = keyValuePairByAggQryBilled(aggregateQuery, bookingColl, 'customer')
                doc[:billedCustomers] = {
                    :xAxis => returnedObj[:key],
                    :yAxis => returnedObj[:value]
                }
            else
                doc[:billedCustomers] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition




            # Repeat/New & Dormant Customers 
            if fiscal_quarter.nil? && fiscal_month.nil? && fiscal_week.nil?
            #if (false)
                prevYear1 = fiscal_year.to_i - 1
                matchObj2 = Marshal.load(Marshal.dump(matchObj))
                matchObj2[:$match]["periods.year"] = prevYear1.to_s
                matchObj2[:$match].delete("periods.quarter")
                matchObj2[:$match].delete("periods.month")
                matchObj2[:$match].delete("periods.week")

                prevYear2 = fiscal_year.to_i - 2
                prevYear3 = fiscal_year.to_i - 3
                puts "Prev -1: #{prevYear1}, Prev - 2: #{prevYear2}, Prev - 3: #{prevYear3}"
                matchObj3 = Marshal.load(Marshal.dump(matchObj2))
                matchObj3[:$match].delete("periods.year")
                matchObj3[:$match][:$or] = [
                    { "periods.year" => prevYear1.to_s },
                    { "periods.year" => prevYear2.to_s },
                    { "periods.year" => prevYear3.to_s },
                ]

                aggregateQuery = [
                    matchObj,
                    dsObj.groupBookingByCustomer(),
                ]
                aggregateQuery2 = [
                    matchObj2,
                    dsObj.groupBookingByCustomer(),
                ]
                aggregateQuery3 = [
                    matchObj3,
                    dsObj.groupBookingByCustomer(),
                ]
                aggregateQueries = {
                    :query1 => aggregateQuery,
                    :query2 => aggregateQuery2,
                    :query3 => aggregateQuery3,
                }
                if !matchObj.nil?
                    returnedObj = keyValuePairByAggQryRepeatNew(aggregateQueries, bookingColl, 'customer')
                    doc[:newAccounts] = {
                        :xAxis => returnedObj[:obj1][:key],
                        :yAxis0 => returnedObj[:obj1][:val],
                        :yAxis => returnedObj[:obj1][:value],
                        :yAxis2 => returnedObj[:obj1][:value2],
                        :yAxis3 => returnedObj[:obj1][:value3],
                        :yAxis4 => returnedObj[:obj2][:value2],
                        :yAxis5 => returnedObj[:obj2][:value3],
                    }
                    doc[:repeatAccounts] = {
                        :xAxis => returnedObj[:obj2][:key],
                        :yAxis0 => returnedObj[:obj2][:val],
                        :yAxis => returnedObj[:obj2][:value],
                        :yAxis2 => returnedObj[:obj1][:value4],
                        :yAxis3 => returnedObj[:obj1][:value5],
                        :yAxis4 => returnedObj[:obj2][:value4],
                        :yAxis5 => returnedObj[:obj2][:value5],
                    }
                    doc[:dormantAccounts] = {
                        :xAxis => returnedObj[:obj3][:key],
                        :yAxis0 => returnedObj[:obj3][:val],
                        :yAxis => returnedObj[:obj3][:value],
                        :yAxis2 => returnedObj[:obj3][:value2],
                        :yAxis3 => returnedObj[:obj3][:value3],
                        :yAxis4 => returnedObj[:obj3][:value4],
                        :yAxis5 => returnedObj[:obj3][:value5],
                        :yAxis6 => returnedObj[:obj3][:value6],
                    }
                else
                    doc[:newAccounts] = {
                        :xAxis => [],
                        :yAxis0 => [],
                        :yAxis => [],
                        :yAxis2 => [],
                        :yAxis3 => [],
                        :yAxis4 => [],
                    } 
                    doc[:repeatAccounts] = {
                        :xAxis => [],
                        :yAxis0 => [],
                        :yAxis => [],
                        :yAxis2 => [],
                        :yAxis3 => [],
                        :yAxis4 => [],
                    } 
                    doc[:dormantAccounts] = {
                        :xAxis => [],
                        :yAxis0 => [],
                        :yAxis => [],
                        :yAxis2 => [],
                        :yAxis3 => [],
                        :yAxis4 => [],
                        :yAxis5 => [],
                        :yAxis6 => [],
                    } 
                end # End of If condition
            else
                doc[:newAccounts] = {
                    :xAxis => [],
                    :yAxis0 => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                    :yAxis4 => [],
                } 
                doc[:repeatAccounts] = {
                    :xAxis => [],
                    :yAxis0 => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                    :yAxis4 => [],
                } 
                doc[:dormantAccounts] = {
                    :xAxis => [],
                    :yAxis0 => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                    :yAxis4 => [],
                    :yAxis5 => [],
                    :yAxis6 => [],
                } 
            end # End of checking whether the period criteria is for only YTD 


            # Billed Partners
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            matchObj[:$match][:$and] = [
                {
                    "names.partner.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupByPartners(),
            ]
            if !matchObj.nil?
                returnedObj = keyValuePairByAggQryBilled(aggregateQuery, bookingColl, 'partner')
                doc[:billedPartners] = {
                    :xAxis => returnedObj[:key],
                    :yAxis => returnedObj[:value]
                }
            else
                doc[:billedPartners] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition


            # Technology Penetration
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByCustomerTechnologies(),
            ]
            if !matchObj.nil?
                returnedObj = keyValuePairByAggQryTechPenetration(aggregateQuery, bookingColl)
                doc[:techPenetration] = {
                    :xAxis => returnedObj[:key],
                    :yAxis => returnedObj[:value],
                    :yAxis2 => [ScalarCalculators.calculateRatio(doc[:tdBooking][:yAxis][0],returnedObj[:value][0]).to_f*THOUSAND]
                }
            else
                doc[:techPenetration] = {
                    :xAxis => [],
                    :yAxis => [],
                    :yAxis2 => []
                } 
            end # End of If condition


            # QoQ Booking
            tempQuarter = nil
            tempMonth = nil
            tempWeek = nil
            matchObj = dsObj.matchByMultipleParams(fiscal_year, tempQuarter, tempMonth, tempWeek, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByQoQ(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:qoqBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:qoqBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # MoM Booking
            tempMonth = nil
            tempWeek = nil
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, tempMonth, tempWeek, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByMoM(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:momBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:momBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # WoW Booking
            tempWeek = nil
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, tempWeek, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByWoW(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:wowBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:wowBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Product Service Booking
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByProductService()
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:prodSerBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:prodSerBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Average Discount of Archs
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingNetAndListByArchs()
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggQryDiscount(aggregateQuery, bookingColl)
                doc[:disArchsBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value],
                    :yAxis2 => bookingObj[:value2],
                    :yAxis3 => bookingObj[:value3],
                }
            else
                doc[:disArchsBooking] = {
                    :xAxis => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                } 
            end # End of If condition

            # Average Discount of Overall 
            aggregateQuery = [
                matchObj,
                dsObj.groupExclusiveBookingNetAndList(),
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggQryDiscount(aggregateQuery, bookingColl)
                doc[:disAllBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value],
                    :yAxis2 => bookingObj[:value2],
                    :yAxis3 => bookingObj[:value3],
                }
            else
                doc[:disAllBooking] = {
                    :xAxis => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                } 
            end # End of If condition

            # Booking History
            tempYear = nil
            matchObj = dsObj.matchByMultipleParams(tempYear, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:prod_ser] = prod_ser
            #ap matchObj
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByHistory(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingHistoryObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                #ap bookingHistoryObj
                doc[:bookingHistory] = {
                    :xAxis => bookingHistoryObj[:key],
                    :yAxis => bookingHistoryObj[:value]
                }
            else
                doc[:bookingHistory] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Yield Per Customer
            if !doc[:tdBooking][:yAxis].empty?
                doc[:yieldPerCustomer] = {
                    :xAxis => [doc[:billedCustomers][:xAxis]],
                    :yAxis => [ScalarCalculators.calculateRatio(doc[:tdBooking][:yAxis][0],doc[:billedCustomers][:yAxis][0]).to_f*THOUSAND] 
                }
            else
                doc[:yieldPerCustomer] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            if !doc[:tdBooking][:yAxis].empty?
                puts "Initializing mongo insertion for #{user_name} | #{fiscal_year} | #{fiscal_quarter} | #{fiscal_month} | #{fiscal_week}"
                id = mongoEng.insertDocument(gdColl, doc) 
                puts "#{user_name} | #{fiscal_year} | #{fiscal_quarter} | #{fiscal_month} | #{fiscal_week} - insertion ID: #{id}"
            end # End of if condition checking if there is any data
            #ap doc
            #puts "#{doc}"
        end # End of generateProdSerDataset method


        # Method to generate data set for general dashboard
        def generateDataset(dataDict)
            user_name = dataDict[:userName]
            fiscal_year = dataDict[:fiscalYear]; fiscal_quarter = dataDict[:fiscalQuarter]
            fiscal_month = dataDict[:fiscalMonth]; fiscal_week = dataDict[:fiscalWeek]
            bookingColl = dataDict[:bookingColl]; gdColl = dataDict[:gdColl];
            mongoEng = dataDict[:mongoEng]; dsObj = dataDict[:dsObj]
            doc = {
                :username => user_name,
                :periods => {
                    :year => fiscal_year,
                    :quarter => fiscal_quarter,
                    :month => fiscal_month,
                    :week => fiscal_week
                }
            }
            # Till date Booking
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByHistory(),
                { :$sort => {:_id => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:tdBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                } 
            else
                doc[:tdBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Architecture wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByArch2(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:archBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:archBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Vertical wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByVertical(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:verticalBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:verticalBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Technology wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByTechName(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:techBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:techBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # AT Attach wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByAtAttach(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:atAttachBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:atAttachBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Sub SCMS wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingBySubSCMS(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:subSCMSBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:subSCMSBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # GTMu wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByGTMu(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:gtmuBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:gtmuBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Region wise Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByRegion(),
                { :$sort => {:booking => -1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:regionBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:regionBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Top 10 Customers Ranking by Booking
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 

            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByCustomer(),
                { :$sort => {:booking => -1}},
                { :$limit => 10 }
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:topCustomerBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:topCustomerBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Top 10 Partner Ranking by Booking
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:$and] = [
                {
                    "names.partner.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByPartner(),
                { :$sort => {:booking => -1}},
                { :$limit => 10 }
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:topPartnerBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:topPartnerBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Top 10 Sales_Level_6 Ranking by Booking
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingBySL6(),
                { :$sort => {:booking => -1}},
                { :$limit => 10 }
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:topSL6Booking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:topSL6Booking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Top 10 Deals
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                },
                {
                    "references.sales_order_number_detail" => {
                        :$ne => nil 
                    }
                },
                {
                    "references.sales_order_number_detail" => {
                        :$ne => "" 
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByCustomerSONumber(),
                { :$sort => {:booking => -1}},
                { :$limit => 10 }
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggQryTopDeals(aggregateQuery, bookingColl)
                doc[:topDeals] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:topDeals] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Billed Customers
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupByCustomers(),
            ]
            if !matchObj.nil?
                returnedObj = keyValuePairByAggQryBilled(aggregateQuery, bookingColl, 'customer')
                doc[:billedCustomers] = {
                    :xAxis => returnedObj[:key],
                    :yAxis => returnedObj[:value]
                }
            else
                doc[:billedCustomers] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition



            # Repeat/New & Dormant Customers 
            if fiscal_quarter.nil? && fiscal_month.nil? && fiscal_week.nil?
            #if (false)
                prevYear1 = fiscal_year.to_i - 1
                matchObj2 = Marshal.load(Marshal.dump(matchObj))
                matchObj2[:$match]["periods.year"] = prevYear1.to_s
                matchObj2[:$match].delete("periods.quarter")
                matchObj2[:$match].delete("periods.month")
                matchObj2[:$match].delete("periods.week")

                prevYear2 = fiscal_year.to_i - 2
                prevYear3 = fiscal_year.to_i - 3
                #puts "Prev -1: #{prevYear1}, Prev - 2: #{prevYear2}, Prev - 3: #{prevYear3}"
                matchObj3 = Marshal.load(Marshal.dump(matchObj2))
                matchObj3[:$match].delete("periods.year")
                matchObj3[:$match][:$or] = [
                    { "periods.year" => prevYear1.to_s },
                    { "periods.year" => prevYear2.to_s },
                    { "periods.year" => prevYear3.to_s },
                ]

                aggregateQuery = [
                    matchObj,
                    dsObj.groupBookingByCustomer(),
                ]
                aggregateQuery2 = [
                    matchObj2,
                    dsObj.groupBookingByCustomer(),
                ]
                aggregateQuery3 = [
                    matchObj3,
                    dsObj.groupBookingByCustomer(),
                ]
                aggregateQueries = {
                    :query1 => aggregateQuery,
                    :query2 => aggregateQuery2,
                    :query3 => aggregateQuery3,
                }
                if !matchObj.nil?
                    returnedObj = keyValuePairByAggQryRepeatNew(aggregateQueries, bookingColl, 'customer')
                    doc[:newAccounts] = {
                        :xAxis => returnedObj[:obj1][:key],
                        :yAxis0 => returnedObj[:obj1][:val],
                        :yAxis => returnedObj[:obj1][:value],
                        :yAxis2 => returnedObj[:obj1][:value2],
                        :yAxis3 => returnedObj[:obj1][:value3],
                        :yAxis4 => returnedObj[:obj2][:value2],
                        :yAxis5 => returnedObj[:obj2][:value3],
                    }
                    doc[:repeatAccounts] = {
                        :xAxis => returnedObj[:obj2][:key],
                        :yAxis0 => returnedObj[:obj2][:val],
                        :yAxis => returnedObj[:obj2][:value],
                        :yAxis2 => returnedObj[:obj1][:value4],
                        :yAxis3 => returnedObj[:obj1][:value5],
                        :yAxis4 => returnedObj[:obj2][:value4],
                        :yAxis5 => returnedObj[:obj2][:value5],
                    }
                    doc[:dormantAccounts] = {
                        :xAxis => returnedObj[:obj3][:key],
                        :yAxis0 => returnedObj[:obj3][:val],
                        :yAxis => returnedObj[:obj3][:value],
                        :yAxis2 => returnedObj[:obj3][:value2],
                        :yAxis3 => returnedObj[:obj3][:value3],
                        :yAxis4 => returnedObj[:obj3][:value4],
                        :yAxis5 => returnedObj[:obj3][:value5],
                        :yAxis6 => returnedObj[:obj3][:value6],
                    }
                else
                    doc[:newAccounts] = {
                        :xAxis => [],
                        :yAxis0 => [],
                        :yAxis => [],
                        :yAxis2 => [],
                        :yAxis3 => [],
                        :yAxis4 => [],
                    } 
                    doc[:repeatAccounts] = {
                        :xAxis => [],
                        :yAxis0 => [],
                        :yAxis => [],
                        :yAxis2 => [],
                        :yAxis3 => [],
                        :yAxis4 => [],
                    } 
                    doc[:dormantAccounts] = {
                        :xAxis => [],
                        :yAxis0 => [],
                        :yAxis => [],
                        :yAxis2 => [],
                        :yAxis3 => [],
                        :yAxis4 => [],
                        :yAxis5 => [],
                        :yAxis6 => [],
                    } 
                end # End of If condition
            else
                doc[:newAccounts] = {
                    :xAxis => [],
                    :yAxis0 => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                    :yAxis4 => [],
                } 
                doc[:repeatAccounts] = {
                    :xAxis => [],
                    :yAxis0 => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                    :yAxis4 => [],
                } 
                doc[:dormantAccounts] = {
                    :xAxis => [],
                    :yAxis0 => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                    :yAxis4 => [],
                    :yAxis5 => [],
                    :yAxis6 => [],
                } 
            end # End of checking whether the period criteria is for only YTD 



            # Billed Partners
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:$and] = [
                {
                    "names.partner.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.partner.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupByPartners(),
            ]
            if !matchObj.nil?
                returnedObj = keyValuePairByAggQryBilled(aggregateQuery, bookingColl, 'partner')
                doc[:billedPartners] = {
                    :xAxis => returnedObj[:key],
                    :yAxis => returnedObj[:value]
                }
            else
                doc[:billedPartners] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition


            # Technology Penetration
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            matchObj[:$match][:$and] = [
                {
                    "names.customer.unique_name" => {
                        :$not => /^unknown/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^small busi/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^cobo una/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^run rate/i
                    }
                },
                {
                    "names.customer.unique_name" => {
                        :$not => /^runrate/i
                    }
                }
            ] 
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByCustomerTechnologies(),
            ]
            if !matchObj.nil?
                returnedObj = keyValuePairByAggQryTechPenetration(aggregateQuery, bookingColl)
                doc[:techPenetration] = {
                    :xAxis => returnedObj[:key],
                    :yAxis => returnedObj[:value],
                    :yAxis2 => [ScalarCalculators.calculateRatio(doc[:tdBooking][:yAxis][0],returnedObj[:value][0]).to_f*THOUSAND]
                }
            else
                doc[:techPenetration] = {
                    :xAxis => [],
                    :yAxis => [],
                    :yAxis2 => []
                } 
            end # End of If condition


            # QoQ Booking
            tempQuarter = nil
            tempMonth = nil
            tempWeek = nil
            matchObj = dsObj.matchByMultipleParams(fiscal_year, tempQuarter, tempMonth, tempWeek, user_name)
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByQoQ(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:qoqBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:qoqBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # MoM Booking
            tempMonth = nil
            tempWeek = nil
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, tempMonth, tempWeek, user_name)
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByMoM(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:momBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:momBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # WoW Booking
            tempWeek = nil
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, tempWeek, user_name)
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByWoW(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:wowBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:wowBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Product Service Booking
            matchObj = dsObj.matchByMultipleParams(fiscal_year, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByProductService()
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                doc[:prodSerBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value]
                }
            else
                doc[:prodSerBooking] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Average Discount of Archs
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingNetAndListByArchs()
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggQryDiscount(aggregateQuery, bookingColl)
                doc[:disArchsBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value],
                    :yAxis2 => bookingObj[:value2],
                    :yAxis3 => bookingObj[:value3],
                }
            else
                doc[:disArchsBooking] = {
                    :xAxis => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                } 
            end # End of If condition

            # Average Discount of Overall 
            aggregateQuery = [
                matchObj,
                dsObj.groupExclusiveBookingNetAndList(),
            ]
            if !matchObj.nil?
                bookingObj = keyValuePairByAggQryDiscount(aggregateQuery, bookingColl)
                doc[:disAllBooking] = {
                    :xAxis => bookingObj[:key],
                    :yAxis => bookingObj[:value],
                    :yAxis2 => bookingObj[:value2],
                    :yAxis3 => bookingObj[:value3],
                }
            else
                doc[:disAllBooking] = {
                    :xAxis => [],
                    :yAxis => [],
                    :yAxis2 => [],
                    :yAxis3 => [],
                } 
            end # End of If condition

            # Booking History
            tempYear = nil
            matchObj = dsObj.matchByMultipleParams(tempYear, fiscal_quarter, fiscal_month, fiscal_week, user_name)
            aggregateQuery = [
                matchObj,
                dsObj.groupBookingByHistory(),
                { :$sort => {:_id => 1}}
            ]
            if !matchObj.nil?
                bookingHistoryObj = keyValuePairByAggregateQuery(aggregateQuery, bookingColl)
                #ap bookingHistoryObj
                doc[:bookingHistory] = {
                    :xAxis => bookingHistoryObj[:key],
                    :yAxis => bookingHistoryObj[:value]
                }
            else
                doc[:bookingHistory] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            # Yield Per Customer
            if !doc[:tdBooking][:yAxis].empty?
                doc[:yieldPerCustomer] = {
                    :xAxis => [doc[:billedCustomers][:xAxis]],
                    :yAxis => [ScalarCalculators.calculateRatio(doc[:tdBooking][:yAxis][0],doc[:billedCustomers][:yAxis][0]).to_f*THOUSAND] 
                }
            else
                doc[:yieldPerCustomer] = {
                    :xAxis => [],
                    :yAxis => []
                } 
            end # End of If condition

            if !doc[:tdBooking][:yAxis].empty?
                #puts "Initializing mongo insertion for #{user_name} | #{fiscal_year} | #{fiscal_quarter} | #{fiscal_month} | #{fiscal_week}"
                id = mongoEng.insertDocument(gdColl, doc) 
                puts "#{user_name} | #{fiscal_year} | #{fiscal_quarter} | #{fiscal_month} | #{fiscal_week} - insertion ID: #{id}"
            end # End of if condition checking if there is any data
            #ap doc
            #puts "#{doc}"
        end # End of generateDataset method

    end # End of SubsetBookingData class
end # End of module GeneralDashboard


