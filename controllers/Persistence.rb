#Module contains all Mongodb Backup dump methods
module Backup
    class BackupDump
        attr_accessor :collections, :path, :database

        def initialize()
            puts "Back up initializing..."
            @collections = %w(general_dashboard general_dashboard_prodser \
            general_dashboard_yoy general_dashboard_prod_yoy \
            general_dashboard_ser_yoy)
            @wPath ="C:\\Jeyaraj\\File_Transfer_Folder\\mongo_backup" 
            @xPath ="C:\\Jeyaraj\\File_Transfer_Folder\\mongo_backup\\truenorth\\" 
            @database = "truenorth"
        end # End of constructor

        # Method to dump all database from W-series Laptop
        def wDumpAllDatabase()
            puts "Back up commencing..."
            Dir.chdir('/Users/jeydurai')
            system('start_mongodump.bat')
        end # End of wDumpAllDatabase method

        # Method to dump all database from X1Carbon Laptop
        def xDumpAllDatabase()
            puts "Back up commencing..."
            Dir.chdir('/Users/jeydurai')
            system('start_mongodump.bat')
        end # End of wDumpAllDatabase method

    end # End of BackDump class

end # End of Backup Module

# Module contains all Mongodb Restore methods
module Restore
    class RestoreDump
        attr_accessor :collections, :path, :database

        def initialize()
            @collections = %w(general_dashboard general_dashboard_prodser \
            general_dashboard_yoy general_dashboard_prod_yoy \
            general_dashboard_ser_yoy)
            @wPath ="C:\\Jeyaraj\\File_Transfer_Folder\\mongo_backup" 
            @xPath ="C:\\Jeyaraj\\File_Transfer_Folder\\mongo_backup\\truenorth\\" 
            @database = "truenorth"
        end # End of constructor

        # Method to restore some specific mongodb collections from X1-Carbon
        # Laptop
        def xRestoreCollections()
            puts "Back up commencing..."
            Dir.chdir('/Users/jeydurai')
            system('start_mongorestore_coll.bat')
        end # End of xRestoreCollections method

        # Method to restore some specific mongodb collections from W-Series Laptop
        def wRestoreCollections()
            puts "Back up commencing..."
            Dir.chdir('/Users/jeydurai')
            system('start_mongorestore_coll.bat')
        end # End of wRestoreCollections method

        # Method to restore all MongoDB collections from X1-Carbon Laptop
        def xRestoreDump()
            puts "Back up commencing..."
            Dir.chdir('/Users/jeydurai')
            system('start_mongorestore.bat')
        end # End of xRestoreDump method

        # Method to restore all MongoDB collections from W-Series Laptop
        def wRestoreDump()
            puts "Back up commencing..."
            Dir.chdir('/Users/jeydurai')
            system('start_mongorestore.bat')
        end # End of wRestoreDump method

    end # End of RestoreDump class

end # End of Restore Module
