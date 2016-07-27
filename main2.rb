# This is the main script to start truenorth_dalet

# Namespace, Module and Class Loading
require './modules/SQLToNoSQLConverter'
require './modules/NoSQLSupporter'
require './modules/truenorth_dalet/AuthorizeUsers'
require './templates/ConsoleGreeter'
require './controllers/CmdController'

include SQLToNoSQLConverter
include NoSQLSupporter
include AuthorizeUsers
include ConsoleGreeter
include CmdController

decObj = Greeter.new()
decObj.printWelcomeMessage()
cmdObj = ConsoleCommand.new()
cmdObj2 = ConsoleHelp.new()

until false # infite loop to make the program run as long as true i.e., command to be quit 
    decObj.printPrompt() # display the command prompt
    commandTxt = gets.chomp # Get command input and store them in a variable

    fullCmdInput = ""
    cmd = ""
    cmdOption = ""

    txtArray = Array.new()
    txtArray = commandTxt.match(/(\w+)(.*)/) # make a match in the input and get them in an array
    if txtArray.nil? # check if the array is null
        decObj.printWrongCommand() # print wrong command message
    else # else meaning that the command input contains strings
        fullCmdInput =  txtArray[0] # saving the full command input
        cmd = txtArray[1] # Saving the command text 
        cmdOption = txtArray[2] # Saving the command option
        if cmd =~ /^quit$/i
            break
        elsif cmd =~ /^auth$/i
            result = cmdObj.cmdAuthExecutor(cmdOption) # Take the control to cmdAuthExecutor method
            if not result
                decObj.printWrongCommand() # print command's wrong option message
            end
        elsif cmd =~ /^cvrt$/i
            result = cmdObj.cmdCvrtExecutor(cmdOption) # Take the control to cmdCvrtExecutor method
            if not result
                decObj.printWrongCommand() # print command's wrong option message
            end
        elsif cmd =~ /^modi$/i
            result = cmdObj.cmdCreateIndexes(cmdOption) # Take the control to cmdCreateIndexes method
            if not result
                decObj.printWrongCommand() # print command's wrong option message
            end
        elsif cmd =~ /^subs$/i
            result = cmdObj.cmdSubsetData(cmdOption) # Take the control to cmdSubsetData method
            if not result
                decObj.printWrongCommand() # print command's wrong option message
            end
        elsif cmd =~ /^bkup$/i
            result = cmdObj.cmdBackupExecutor(cmdOption) # Take the control to cmdBackupExecutor method
            if not result
                decObj.printWrongCommand() # print command's wrong option message
            end
        elsif cmd =~ /^rstr$/i
            result = cmdObj.cmdRestoreExecutor(cmdOption) # Take the control to cmdRestoreExecutor method
            if not result
                decObj.printWrongCommand() # print command's wrong option message
            end
        elsif cmd =~ /^help$/i
            result = cmdObj2.cmdHelp(cmdOption) # Take the control to cmdSubsetData method
            if not result
                decObj.printWrongCommand() # print command's wrong option message
            end
        else
            decObj.printWrongCommand() # in case of no match, print wrong command message
        end # End of command text matching if conditions
    end # End of txtArray if condition
end # End of until loop
decObj.printExitMessage()

