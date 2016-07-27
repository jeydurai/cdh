module CmdController

    # Namespace, Module  and Class Loading
    require './modules/SQLToNoSQLConverter'
    require './modules/NoSQLSupporter'
    require './modules/truenorth_dalet/AuthorizeUsers'
    require './templates/ConsoleGreeter'
    require './controllers/CmdController'
    require './controllers/Persistence'
    require './modules/truenorth_dalet/GeneralDashboard'

    include SQLToNoSQLConverter
    include NoSQLSupporter
    include AuthorizeUsers
    include GeneralDashboard
    include Backup
    include Restore

    class ConsoleCommand

        # Method to check Options and execute Authorization commands
        def cmdAuthExecutor(option)
            txtArray = option.match(/-(.*)/)
            if txtArray.nil? #Extract the option string except hyphen
                return false # if there be no match, return to main to continue
            else
                optionString = txtArray[1]
                if optionString =~ /^tn:usr-act$/i
                    print "\nUser Name: "
                    userName = gets.chomp # get the user name from the command prompt
                    obj = ActivateUsers.new()
                    obj.authorizeApprovedUser(userName)
                elsif optionString =~ /^tn:usr-act-all$/i
                    puts "\nActivate all Users for truenorth app\n"
                    obj = ActivateUsers.new()
                    obj.authorizeAllApprovedUsers()
                elsif optionString =~ /^tn:usr-dac$/i
                    print "\nUser Name: "
                    userName = gets.chomp # get the user name from the command prompt
                    puts "\nUser Name entered is #{userName}\n"
                elsif optionString =~ /^tn:usr-ref$/i
                    puts "\nRollback all Users Authentication\n"
                    obj = DeactivateUsers.new()
                    obj.refreshMappedToField()
                elsif optionString =~ /^tn:usr-res$/i
                    puts "\nRollback all Users Activation\n"
                    obj = DeactivateUsers.new()
                    obj.deactivateUsers()
                else
                    return false # if not the option string match, return to main to continue
                end # End of option checking and execution of respective methods
                return true
            end # End of If to extract option string
        end # End of cmdAuthExecutor method

        # Method to check Options and execute Converter commands
        def cmdCvrtExecutor(option)
            txtArray = option.match(/-(.*)/)
            if txtArray.nil? #Extract the option string except hyphen
                return false # if there be no match, return to main to continue
            else
                optionString = txtArray[1]
                if optionString =~ /^fd:xlsq-sq:sq-nsq$/i
                    puts "\nRun Findump conversion from XL to SQL and SQL to NoSQL\n"
                    obj = ConvertAsWhole.new()
                    obj.convertAndWrite()
                elsif optionString =~ /^bd:sq-nsq$/i
                    puts "\nRun Findump Snap Shot conversion from SQL to NoSQL\n"
                    obj = ConvertAsWhole.new()
                    obj.convertAsSnapshotAndWrite()
                elsif optionString =~ /^fd:xlsq-init$/i
                    puts "\nRun Initializing SQL/NoSQL Datasource \n"
                    obj = ConvertAsWhole.new()
                    obj.cleanInit()
                else
                    return false # if not the option string match, return to main to continue
                end # End of option checking and execution of respective methods
                return true
            end # End of If to extract option string
        end # End of cmdCvrtExecutor mehtod

        # Method to check Options and execute Backup 
        def cmdBackupExecutor(option)
            txtArray = option.match(/-(.*)/)
            if txtArray.nil? #Extract the option string except hyphen
                return false # if there be no match, return to main to continue
            else
                optionString = txtArray[1]
                if optionString =~ /^all:nsq-nsq-w$/i
                    puts "\nRun Backup in Mongodb from W-series laptop\n"
                    obj = BackupDump.new()
                    obj.wDumpAllDatabase()
                elsif optionString =~ /^all:nsq-nsq-x$/i
                    puts "\nRun Backup in Mongodb from X1-Carbon laptop\n"
                    obj = BackupDump.new()
                    obj.xDumpAllDatabase()
                else
                    return false # if not the option string match, return to main to continue
                end # End of option checking and execution of respective methods
                return true
            end # End of If to extract option string
        end # End of cmdBackupExecutor mehtod

        # Method to check Options and execute Restore 
        def cmdRestoreExecutor(option)
            txtArray = option.match(/-(.*)/)
            if txtArray.nil? #Extract the option string except hyphen
                return false # if there be no match, return to main to continue
            else
                optionString = txtArray[1]
                if optionString =~ /^coll:nsq-nsq-w$/i
                    puts "\nRun Restore set of collections in Mongodb from W-series laptop\n"
                    obj = RestoreDump.new()
                    obj.wRestoreCollections()
                elsif optionString =~ /^coll:nsq-nsq-x$/i
                    puts "\nRun Restore set of collections in Mongodb from X1-Carbon laptop\n"
                    obj = RestoreDump.new()
                    obj.xRestoreCollections()
                elsif optionString =~ /^all:nsq-nsq-w$/i
                    puts "\nRun Restore Dump in Mongodb from W-Series laptop\n"
                    obj = RestoreDump.new()
                    obj.wRestoreDump()
                elsif optionString =~ /^all:nsq-nsq-x$/i
                    puts "\nRun Restore Dump in Mongodb from X1-Carbon laptop\n"
                    obj = RestoreDump.new()
                    obj.xRestoreDump()
                else
                    return false # if not the option string match, return to main to continue
                end # End of option checking and execution of respective methods
                return true
            end # End of If to extract option string
        end # End of cmdBackupExecutor mehtod

        def cmdSubsetData(option)
            txtArray = option.match(/-(.*)/)
            if txtArray.nil? #Extract the option string except hyphen
                return false # if there be no match, return to main to continue
            else
                optionString = txtArray[1]
                if optionString =~ /^bd:nsq-nsq-und$/i
                    puts "\nSub set NoSQL Booking Data to write Uniquenodes in NoSQL\n"
                    obj = SubSetAndWrite.new()
                    obj.getAndWriteUniqueNodesAll()
                elsif optionString =~ /^bd:nsq-nsq-gd$/i
                    puts "\nSubset NoSQL Booking Data to write summarized general dashboard data\n"
                    obj = GeneralDashboard::SubsetBookingData.new()
                    obj.runSubset()
                elsif optionString =~ /^gd:nsq-nsq-gdyoy$/i
                    puts "\nSubset NoSQL General Dashboard Data to write YoY in general_dashboard2 data\n"
                    obj = GeneralDashboard::SubsetBookingData.new()
                    obj.runSubsetYoY()
                elsif optionString =~ /^bd:nsq-nsq-gdps$/i
                    puts "\nSubset NoSQL Booking Data to write summarized general dashboard data for Product and Service\n"
                    obj = GeneralDashboard::SubsetBookingData.new()
                    obj.runProdSerSubset()
                elsif optionString =~ /^gd:nsq-nsq-gdprodyoy$/i
                    puts "\nSubset NoSQL General Dashboard Data to write YoY in general_dashboard2 data for Product and Service\n"
                    obj = GeneralDashboard::SubsetBookingData.new()
                    obj.runProductSubsetYoY()
                elsif optionString =~ /^gd:nsq-nsq-gdservyoy$/i
                    puts "\nSubset NoSQL General Dashboard Data to write YoY in general_dashboard2 data for Product and Service\n"
                    obj = GeneralDashboard::SubsetBookingData.new()
                    obj.runServiceSubsetYoY()
                else
                    return false # if not the option string match, return to main to continue
                end # End of option checking and execution of respective methods
                return true
            end # End of If to extract option string
        end # End of cmdCvrtExecutor mehtod
        
        
        # Method to check Options and execute Index commands
        def cmdCreateIndexes(option)
            txtArray = option.match(/-(.*)/)
            if txtArray.nil? #Extract the option string except hyphen
                return false # if there be no match, return to main to continue
            else
                optionString = txtArray[1]
                if optionString =~ /^bd:nsq-idx$/i
                    puts "\nCreate Indexes in NoSQL Booking Dump\n"
                    obj = IndexHandler.new()
                    obj.createAllIndexes()
                elsif optionString =~ /^bd:nsq-idx-rm$/i
                    puts "\nRemove all Indexes in NoSQL Booking Dump\n"
                    obj = IndexHandler.new()
                    obj.removeAllIndexes()
                elsif optionString =~ /^gd:nsq-idx$/i
                    puts "\nCreate Indexes in NoSQL General Dashboard Data\n"
                elsif optionString =~ /^gd:nsq-idx-rm$/i
                    puts "\nRemove all Indexes in NoSQL General Dashboard Data\n"
                else
                    return false # if not the option string match, return to main to continue
                end # End of option checking and execution of respective methods
                return true
            end # End of If to extract option string
        end # End of cmdCvrtExecutor mehtod
        
    end # End of ConsoleCommand class

    class ConsoleHelp # container to have all the help commands

        # Method to display help - show all the commands available 
        def cmdHelp(option)
            txtArray = option.match(/-(.*)/)
            if txtArray.nil? #Extract the option string except hyphen
                puts "\nCommands available\n"
                puts "#{'=' * 18}\n"
                puts "<command> <table>:<fileFormat>-<fileFormat>-[action]\n"
                puts "'auth': Authorization command\n"
                puts "'cvrt': Conversion command\n"
                puts "'modi': Modification/Edit command\n"
                puts "'subs': Subset Data command\n"
                puts "'bkup': Data Backup command\n"
                puts "'rstr': Data Restore command\n"
                puts "'help': To list all available commands in CDH\n"
                puts "#{'=' * 100}\n"
                return true
            else
                optionString = txtArray[1]
                if optionString =~ /^cvrt$/i
                    puts "\nConverter Command\n"
                    puts "#{'=' * 20}\n"
                    puts "What is it for? : To convert any form of data to a specific form\n"
                    puts "Options Available:\n"
                    puts "#{'-' * 18}\n"
                    puts "<table>:<fileFormat>-<fileFormat>-[action]\n"
                    puts "'fd:xlsq-sq:sq-nsq': used to convert finance dump from XL to SQL and SQL to NoSQL (MongoDB)\n"
                    puts "'bd:sq-nsq': used to convert booking dump from SQL to NoSQL in a snapshot (MongoDB)\n"
                    puts "'fd:xlsq-init': used to initialize the datasource for data conversion\n"
                    puts "#{'=' * 100}\n"
                elsif optionString =~ /^auth$/i
                    puts "\Authorization Command\n"
                    puts "#{'=' * 25}\n"
                    puts "What is it for? : To authorize TrueNorth Data\n"
                    puts "Options Available:\n"
                    puts "#{'-' * 18}\n"
                    puts "<table>:<fileFormat>-<fileFormat>-[action]\n"
                    puts "'tn:usr-act': to authorize/activate/map a user to the booking_dump table\n"
                    puts "'tn:usr-act-all': to authorize/activate/map all users to the booking_dump table\n"
                    puts "'tn-usr-dac': to unauthorize/deactivate/unmap a users to the booking_dump table\n"
                    puts "'tn:usr-ref': to refresh all users by unmap the MapTo field in the booking_dump table\n"
                    puts "'tn:usr-res': to rollback all users' activation by unset the approval status code to 2 in Users collection\n"
                    puts "#{'=' * 100}\n"
                elsif optionString =~ /^modi$/i
                    puts "\nModification/Edit Command\n"
                    puts "#{'=' * 25}\n"
                    puts "What is it for? : To create Indexes and do other modifications in booking & dashboard data\n"
                    puts "Options Available:\n"
                    puts "#{'-' * 18}\n"
                    puts "<table>:<fileFormat>-<fileFormat>-[action]\n"
                    puts "'bd:nsq-idx': to create indexes in booking dump collection in MongoDB\n"
                    puts "'bd:nsq-idx-rm': to remove indexes from booking dump collection in MongoDB\n"
                    puts "'gd:nsq-idx': to create indexes in general dashboard data collection in MongodDB\n"
                    puts "#{'=' * 100}\n"
                elsif optionString =~ /^subs$/i
                    puts "\nSubset Data Command\n"
                    puts "#{'=' * 25}\n"
                    puts "What is it for? : To subset data from booking dump, goal sheet, funnel report\n"
                    puts "Options Available:\n"
                    puts "#{'-' * 18}\n"
                    puts "<table>:<fileFormat>-<fileFormat>-[action]\n"
                    puts "'bd:nsq-nsq-und': to subset unique nodes from booking dump and write it in separate collection\n"
                    puts "'bd:nsq-nsq-gd': to subset general dashboard data from booking dump and write it in separate collection\n"
                    puts "'gd:nsq-nsq-gdyoy': to subset YoY data from general_dashboard data and write it in separate collection\n"
                    puts "'bd:nsq-nsq-gdps': to subset Product/Service general dashboard data from booking dump and write it in separate collection\n"
                    puts "'gd:nsq-nsq-gdprodyoy': to subset YoY data from Product general_dashboard data and write it in separate collection\n"
                    puts "'gd:nsq-nsq-gdservyoy': to subset YoY data from Service general_dashboard data and write it in separate collection\n"
                    puts "#{'=' * 100}\n"
                elsif optionString =~ /^bkup$/i
                    puts "\nData Backup Command\n"
                    puts "#{'=' * 25}\n"
                    puts "What is it for? : To take backup in MongoDB for 'truenorth' database\n"
                    puts "Options Available:\n"
                    puts "#{'-' * 18}\n"
                    puts "<table>:<fileFormat>-<fileFormat>-[action]\n"
                    puts "'all:nsq-nsq-w': to take backup of all collections in truenorth database from W-Series Laptop\n"
                    puts "'all:nsq-nsq-x': to take backup of all collections in truenorth database from X1-Carbon Laptop\n"
                    puts "#{'=' * 100}\n"
                elsif optionString =~ /^rstr$/i
                    puts "\nData Restore Command\n"
                    puts "#{'=' * 25}\n"
                    puts "What is it for? : To Restore Dump in MongoDB for 'truenorth' database\n"
                    puts "Options Available:\n"
                    puts "#{'-' * 18}\n"
                    puts "<table>:<fileFormat>-<fileFormat>-[action]\n"
                    puts "'coll:nsq-nsq-w': to restore set of collections in truenorth database from W-Series Laptop\n"
                    puts "'coll:nsq-nsq-x': to restore set of collections in truenorth database from X1-Carbon Laptop\n"
                    puts "'all:nsq-nsq-w': to restore all collections in truenorth database from W-Series Laptop\n"
                    puts "'all:nsq-nsq-x': to restore all collections in truenorth database from X1-Carbon Laptop\n"
                    puts "#{'=' * 100}\n"
                else
                    return false # if there be no match, return to main to continue
                end # End of option checking and execution of respective methods
                return true
            end # End of If to extract option string
        end # End of cmdHelp method 
        
    end # End of ConsoleHelp class

end # End of CmdController module
