#= Console Greeter Module
module ConsoleGreeter
    # Space for Namespaces, Modules and Class loading...
   
   class Greeter
       attr_accessor :underLine, :preDecorator, :postDecorator, \
           :preDecorator2, :preDecorator3, :preDecorator4, \
           :preDecorator5, :preDecorator6, :preDecorator7, \
           :preDecModifiable, :postDecModifiable

       # initializer to store underline decorator in a variable
       def initialize()
           @underLine = '=' * 100
           @preDecorator = '*' * 35  
           @preDecModifiable = '*' * 30
           @preDecorator2 = '#' * 6
           @preDecorator3 = '#' * 5
           @preDecorator4 = '#' * 4
           @preDecorator5 = '#' * 3
           @preDecorator6 = '#' * 2
           @preDecorator7 = '#' * 1
           @postDecorator = '*' * 35
           @postDecModifiable = '*' * 30
       end # End of initialize method

       # Method to print Welcome Message
       def printWelcomeMessage()
           puts "#{@underLine}\n\n"
           puts "#{@preDecorator} WELCOME TO CISCO_DATA_MINER #{@postDecorator}\n\n"
           puts "#{@underLine}\n"
           puts "#{@preDecorator2} Version 1.01.01\n#{@preDecorator3} Unique Data Handling Console Application\n"
           puts "#{@preDecorator4} Owner: D. Jeyaraj\n#{@preDecorator5} Division: Commercial Sales\n#{@preDecorator6} Profile: Data Analytics\n"
           puts "#{@preDecorator7} CDH is meant to be an Internal Console Applicaton which does not have any copyright\n"
           puts "#{@underLine}\n\n"
       end # End of printWelcomeMessage method

       # Method to display wrong command input
       def printWrongCommand()
            puts "Unrecognized/Bad CDH command!\n"
       end # End of printWrongCommand method

       # Method to show prompt
       def printPrompt()
           print "\nCDH::jeydurai\@cisco.com> "
       end # End of printPrompt method

       # Method to print Exit Message
       def printExitMessage()
           puts "\n\n#{@underLine}\n\n"
           puts "#{@preDecModifiable} THANK YOU FOR USING CISCO_DATA_MINER #{@postDecModifiable}\n\n"
           puts "#{@underLine}\n"
           puts "#{@preDecorator2} Hope you have had fun using CDH\n#{@preDecorator3} CDH is still under modern development\n"
           puts "#{@preDecorator4} New Versions will have more features that will ease you in accomplishing your data oriented job\n"
           puts "#{@preDecorator5} Current features include NoSQL converter, Truenorth Data Preparer and Admin Handler...\n#{@preDecorator6} 'Fear of the Lord is the beginning of Knowledge'\n"
           puts "#{@preDecorator7} You can write to me your feedbacks to jeydurai\@cisco.com\n"
           puts "#{@underLine}\n\n"
       end # End of printExitMessage method
   end # End of class Greeter 
end # End of Module Console Greeter
