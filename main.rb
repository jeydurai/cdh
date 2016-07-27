# This is a ruby script to fetch all unique nodes
# from *MySQL* database and write it in MongoDB

# Namespace, Module  and Class Loading
require './modules/SQLToNoSQLConverter'
require './modules/NoSQLSupporter'
require './modules/truenorth_dalet/AuthorizeUsers'

include SQLToNoSQLConverter
include NoSQLSupporter
include AuthorizeUsers

ARGV.each do |a|
  if (a == "-sub") or (a == "--subset")
    obj = SubSetAndWrite.new()
    obj.getAndWriteUniqueNodes()
    break
  end
  if (a == "-cvrt") or (a == "--convert")
    obj = ConvertAsWhole.new()
    obj.convertAndWrite()
    break
  end
  if (a == "-idx") or (a == "--index")
    puts "Enters into Index Mode...\n"
    obj = IndexHandler.new()
    obj.createAllIndexes()
    break
  end
  if (a == "-ridx") or (a == "--removeindex")
    puts "Enters into Index Mode...\n"
    obj = IndexHandler.new()
    obj.removeAllIndexes()
    break
  end
  if (a == "-auth") or (a == "--author")
    obj = ActivateUsers.new()
    obj.authorizeAllApprovedUsers()
    break
  end
end

