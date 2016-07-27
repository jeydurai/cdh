module DataStructures

    # Namespace, Module and Class Loading



    class BookingDumpDS

        attr_accessor :fieldYear, :fieldQuarter, :fieldMonth,
            :fieldWeek, :fieldArch2, :fieldTechName, :fieldAtAttach,
            :fieldVertical, :fieldSCMS, :fieldSubSCMS, :fieldGTMu, 
            :fieldRegion, :fieldSL6, :fieldSalesAgent, :fieldPartnerName, 
            :fieldCustomerName, :fieldProdSer, :fieldMappedTo, :fieldBookingNet, :fieldBookingList, :fieldSONumber,
            :fieldYearAsGroup, :fieldQuarterAsGroup, :fieldMonthAsGroup, 
            :fieldWeekAsGroup, :fieldArch2AsGroup, :fieldTechNameAsGroup, 
            :fieldAtAttachAsGroup, :fieldBookingNetAsGroup, :fieldBookingListAsGroup, 
            :fieldSCMSAsGroup, :fieldSubSCMSAsGroup, :fieldGTMuAsGroup, 
            :fieldRegionAsGroup, :fieldSL6AsGroup, :fieldSalesAgentAsGroup, 
            :fieldPartnerNameAsGroup, :fieldCustomerNameAsGroup, :fieldProdSerAsGroup, :fieldSONumberAsGroup,
            :fieldVerticalAsGroup, :fieldMappedToAsGroup

        def initialize()
            # initialize the fields with their constants
            @fieldYear = "periods.year"
            @fieldQuarter = "periods.quarter"
            @fieldMonth = "periods.month"
            @fieldWeek = "periods.week"
            @fieldArch2 = "technologies.arch2"
            @fieldTechName = "technologies.tech_name"
            @fieldAtAttach = "technologies.at_attach"
            @fieldVertical = "business_nodes.industry_vertical"
            @fieldSCMS = "business_nodes.scms"
            @fieldSubSCMS = "business_nodes.sub_scms"
            @fieldGTMu = "location_nodes.gtmu"
            @fieldRegion = "location_nodes.region"
            @fieldSL6 = "location_nodes.sales_level_6"
            @fieldSalesAgent = "names.sales_agent.name"
            @fieldPartnerName = "names.partner.unique_name"
            @fieldCustomerName = "names.customer.unique_name"
            @fieldProdSer = "prod_ser"
            @fieldMappedTo = "mappedTo"
            @fieldBookingNet = "metric.booking_net"
            @fieldBookingList = "metric.base_list"
            @fieldSONumber= "references.sales_order_number_detail"
            
            # Mongo Field Structure as Group Assignment
            @fieldYearAsGroup = "$periods.year"
            @fieldQuarterAsGroup = "$periods.quarter"
            @fieldMonthAsGroup = "$periods.month"
            @fieldWeekAsGroup = "$periods.week"
            @fieldArch2AsGroup = "$technologies.arch2"
            @fieldTechNameAsGroup = "$technologies.tech_name"
            @fieldAtAttachAsGroup = "$technologies.at_attach"
            @fieldBookingNetAsGroup = "$metric.booking_net"
            @fieldBookingListAsGroup = "$metric.base_list"
            @fieldSCMSAsGroup = "$business_nodes.scms"
            @fieldSubSCMSAsGroup = "$business_nodes.sub_scms"
            @fieldGTMuAsGroup = "$location_nodes.gtmu"
            @fieldRegionAsGroup = "$location_nodes.region"
            @fieldSL6AsGroup = "$location_nodes.sales_level_6"
            @fieldSalesAgentAsGroup = "$names.sales_agent.name"
            @fieldPartnerNameAsGroup = "$names.partner.unique_name"
            @fieldCustomerNameAsGroup = "$names.customer.unique_name"
            @fieldProdSerAsGroup = "$prod_ser"
            @fieldVerticalAsGroup = "$business_nodes.industry_vertical"
            @fieldMappedToAsGroup = "$mappedTo"
            @fieldSONumberAsGroup= "$references.sales_order_number_detail"
        end # End of constructor

        # function to return Object for summing up Booking Net   
        
        # Aggregation Group by JSON for Latest Year
        def getGroupObjMaxYear()
            jsonObj = {
                :$max => @fieldYearAsGroup
            } 
            return jsonObj
        end


        # Aggregation Group by JSON for Latest Quarter
        def getGroupObjMaxQuarter()
            jsonObj = {
                :$max => @fieldQuarterAsGroup
            } 
            return jsonObj
        end

        # Aggregation Group by JSON for Latest Month
        def getGroupObjMaxMonth()
            jsonObj = {
                :$max => @fieldMonthAsGroup
            } 
            return jsonObj
        end

        # Aggregation Group by JSON for Latest Week
        def getGroupObjMaxWeek()
            jsonObj = {
                :$max => @fieldWeekAsGroup
            } 
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net
        def getGroupObjBooking()
            jsonObj = {
                :$sum => @fieldBookingNetAsGroup
            } 
            return jsonObj
        end

        # Aggregation Group by JSON for Base List
        def getGroupObjBaseList()
            jsonObj = {
                :$sum => @fieldBookingListAsGroup
            } 
            return jsonObj
        end
        
        # Matching Criteria (OR Query)
        def matchByYear(fiscal_year)
            puts "It is inside matchBYYear method"
            puts @fieldYear
            jsonObj = {
                :$match => {@fieldYear => fiscal_year}
            }
            return jsonObj
        end

        # Matching Criteria by year & quarter
        def matchByYearQuarter(fiscal_year, quarter)
            jsonObj = {
                :$match => {
                    @fieldYear => fiscal_year,
                    @fieldQuarter => quarter,
                }
            }
            return jsonObj
        end

        # Matching Criteria by year, quarter & month
        def matchByYearQuarterMonth(fiscal_year, quarter, month)
            jsonObj = {
                :$match => {
                    @fieldYear => fiscal_year,
                    @fieldQuarter => quarter,
                    @fieldMonth => month,
                }
            }
            return jsonObj
        end

        # Aggregate Group by Customers
        def groupByCustomers()
            jsonObj = {
                :$group => {
                    :_id => @fieldCustomerNameAsGroup
                }
            }
            return jsonObj
        end

        # Aggregate Group by Partners
        def groupByPartners()
            jsonObj = {
                :$group => {
                    :_id => @fieldPartnerNameAsGroup
                }
            }
            return jsonObj
        end


        # Aggregate Group by Periods (year, quarter, month & week)
        def groupByPeriods()
            jsonObj = {
                :$group => {
                    :_id => {
                        :fiscal_year => @fieldYearAsGroup,
                        :fiscal_quarter => @fieldQuarterAsGroup,
                        :fiscal_month => @fieldMonthAsGroup,
                        :fiscal_week => @fieldWeekAsGroup,
                    },
                }
            }
            return jsonObj
        end


        # Aggregate Group by Customers and SO_Number for top deals
        def groupBookingByCustomerSONumber()
            jsonObj = {
                :$group => {
                    :_id => {
                        :customers => @fieldCustomerNameAsGroup,
                        :soNumbers => @fieldSONumberAsGroup,
                    },
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregate Group by Customers and Technologies for Tech Penetration
        def groupBookingByCustomerTechnologies()
            jsonObj = {
                :$group => {
                    :_id => {
                        :customers => @fieldCustomerNameAsGroup,
                        :techs => @fieldTechNameAsGroup,
                    },
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Arch2
        def groupBookingByArch2()
            jsonObj = {
                :$group => {
                    :_id => @fieldArch2AsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Technology
        def groupBookingByTechName()
            jsonObj = {
                :$group => {
                    :_id => @fieldTechNameAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by AT Attach
        def groupBookingByAtAttach()
            jsonObj = {
                :$group => {
                    :_id => @fieldAtAttachAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Sub_SCMS
        def groupBookingBySubSCMS()
            jsonObj = {
                :$group => {
                    :_id => @fieldSubSCMSAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by GTMu
        def groupBookingByGTMu()
            jsonObj = {
                :$group => {
                    :_id => @fieldGTMuAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Region
        def groupBookingByRegion()
            jsonObj = {
                :$group => {
                    :_id => @fieldRegionAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net Exclusive
        def groupExclusiveBooking()
            jsonObj = {
                :$group => {
                    :_id => nil,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Latest year Exclusive
        def groupMaxYear()
            jsonObj = {
                :$group => {
                    :_id => nil,
                    :period => getGroupObjMaxYear() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Latest Quarter Exclusive
        def groupMaxQuarter()
            jsonObj = {
                :$group => {
                    :_id => nil,
                    :period => getGroupObjMaxQuarter() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Latest Month Exclusive
        def groupMaxMonth()
            jsonObj = {
                :$group => {
                    :_id => nil,
                    :period => getGroupObjMaxMonth() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Latest Week Exclusive
        def groupMaxWeek()
            jsonObj = {
                :$group => {
                    :_id => nil,
                    :period => getGroupObjMaxWeek() 
                }
            }
            return jsonObj
        end

        
        # Aggregation Group by JSON for Booking Net & Base List Exclusive
        def groupExclusiveBookingNetAndList()
            jsonObj = {
                :$group => {
                    :_id => nil,
                    :booking => getGroupObjBooking(), 
                    :base_list => getGroupObjBaseList() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net History
        def groupBookingByHistory()
            jsonObj = {
                :$group => {
                    :_id => @fieldYearAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by QoQ
        def groupBookingByQoQ()
            jsonObj = {
                :$group => {
                    :_id => @fieldQuarterAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by MoM
        def groupBookingByMoM()
            jsonObj = {
                :$group => {
                    :_id => @fieldMonthAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by WoW
        def groupBookingByWoW()
            jsonObj = {
                :$group => {
                    :_id => @fieldWeekAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Industry Vertical
        def groupBookingByVertical()
            jsonObj = {
                :$group => {
                    :_id => @fieldVerticalAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Customer
        def groupBookingByCustomer()
            jsonObj = {
                :$group => {
                    :_id => @fieldCustomerNameAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Partner
        def groupBookingByPartner()
            jsonObj = {
                :$group => {
                    :_id => @fieldPartnerNameAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end
        
        # Aggregation Group by JSON for Booking Net by SL6
        def groupBookingBySL6()
            jsonObj = {
                :$group => {
                    :_id => @fieldSL6AsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Product/Service
        def groupBookingByProductService()
            jsonObj = {
                :$group => {
                    :_id => @fieldProdSerAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Quarters
        def groupBookingByQuarters()
            jsonObj = {
                :$group => {
                    :_id => @fieldQuarterAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Month
        def groupBookingByMonths()
            jsonObj = {
                :$group => {
                    :_id => @fieldMonthAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net by Month
        def groupBookingByWeeks()
            jsonObj = {
                :$group => {
                    :_id => @fieldWeekAsGroup,
                    :booking => getGroupObjBooking() 
                }
            }
            return jsonObj
        end

        # Aggregation Group by JSON for Booking Net & Base List by Arch2
        def groupBookingNetAndListByArchs()
            jsonObj = {
                :$group => {
                    :_id => @fieldArch2AsGroup,
                    :booking => getGroupObjBooking(), 
                    :base_list => getGroupObjBaseList(), 
                }
            }
            return jsonObj
        end

        # Matching Criteria by multiple parameters
        def matchByMultipleParams(fiscal_year=nil, quarter=nil, fiscal_month=nil, fiscal_week=nil, user_name=nil)
            jsonObj = {}
            if !fiscal_year.nil?
                if !quarter.nil?
                    if !fiscal_month.nil?
                        if !fiscal_week.nil?
                            jsonObj = {
                                :$match => {
                                    @fieldYear => fiscal_year,
                                    @fieldQuarter => quarter,
                                    @fieldMonth => fiscal_month,
                                    @fieldWeek => fiscal_week,
                                }
                            }
                        else
                            jsonObj = {
                                :$match => {
                                    @fieldYear => fiscal_year,
                                    @fieldQuarter => quarter,
                                    @fieldMonth => fiscal_month
                                }
                            }
                        end
                    else
                        jsonObj = {
                            :$match => {
                                @fieldYear => fiscal_year,
                                @fieldQuarter => quarter
                            }
                        }
                    end
                else
                    jsonObj.update({
                        :$match => {@fieldYear => fiscal_year}
                    })
                end
            elsif !quarter.nil?
                if !fiscal_month.nil?
                    if !fiscal_week.nil?
                        jsonObj = {
                            :$match => {
                                @fieldQuarter => quarter,
                                @fieldMonth => fiscal_month,
                                @fieldWeek => fiscal_week,
                            }
                        }
                    else
                        jsonObj = {
                            :$match => {
                                @fieldQuarter => quarter,
                                @fieldMonth => fiscal_month
                            }
                        }
                    end
                else
                    jsonObj = {
                        :$match => {
                            @fieldQuarter => quarter
                        }
                    }
                end
            elsif !fiscal_month.nil?
                if !fiscal_week.nil?
                    jsonObj = {
                        :$match => {
                            @fieldMonth => fiscal_month,
                            @fieldWeek => fiscal_week,
                        }
                    }
                else
                    jsonObj = {
                        :$match => {
                            @fieldMonth => fiscal_month
                        }
                    }
                end
            elsif !fiscal_week.nil?
                jsonObj = {
                    :$match => {
                        @fieldWeek => fiscal_week,
                    }
                }
            else
                jsonObj = nil
            end

                
            # Merging User Name object
            jsonObj2 = {}

            if !jsonObj.nil?
                if !user_name.nil?
                    jsonObj[:$match][:mappedTo] = user_name
                end
            else
                if !user_name.nil?
                    jsonObj = {
                        :$match => {
                            :mappedTo => user_name
                        }
                    }
                end
            end
            return jsonObj
        end
        
        # =================================================
        
        
    end # End of class BookingDumpDS

end # End of module DataStructures
