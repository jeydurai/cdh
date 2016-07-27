module ScalarCalculators
    
    # Calculate Discount
    def calculateDiscount(bookingNet, baseList)
        discount = 0
        bookingNet = bookingNet.to_f; baseList = baseList.to_f
        if baseList != 0
            discount = 1-(bookingNet/baseList)
        end
        return discount.to_f
    end # End of calculateDiscount method
    
    # Calculate Growth between two scalars
    def ScalarCalculators.calculateGrowth(current, prev)
        growth = 0
        current = current.to_f; prev = prev.to_f
        if ((prev != 0) && !(current < 0 && prev > 0) && !(current > 0 && prev < 0))
            growth = (current - prev) / prev
        end
        return growth.to_f
    end # End of calculateDiscount method


    # Calculate Ratio
    def ScalarCalculators.calculateRatio(numero, deno)
        ratio = 0
        if (deno != 0 && (numero.is_a? Numeric) && (deno.is_a? Numeric))
            numero = numero.to_f; deno = deno.to_f
            ratio = numero / deno
        end
        return ratio.to_f
    end # End of calculateRatio method

end # End of Calculator module

module ArrayCalculators
    # Calculate Growth between two arrays
    def ArrayCalculators.calculateGrowth(current, prev)
        tempXAxis = []; tempYAxis = []
        currentXAxis = current[:xAxis]
        currentYAxis = current[:yAxis]
        prevXAxis = prev[:xAxis]
        prevYAxis = prev[:yAxis]
        arraySize = current[:xAxis].size
        arrayPrevSize = prev[:xAxis].size
        arraySize.times do |i|
            arrayPrevSize.times do |j|
                if current[:xAxis][i] == prev[:xAxis][j]
                    tempXAxis[i] = prev[:xAxis][j]
                    tempYAxis[i] = ScalarCalculators.calculateGrowth(current[:yAxis][i], prev[:yAxis][j])
                end 
            end # End of arrayPrevSize Iteration
        end # End of arraySize Iteration
        returnData = {
            :xAxis => tempXAxis,
            :yAxis => tempYAxis,
            :current => {
                :xAxis => current[:xAxis],
                :yAxis => current[:yAxis]
            },
            :prev => {
                :xAxis => prev[:xAxis],
                :yAxis => prev[:yAxis]
            } 
        }
    end # End of calculateDiscount method

    # Sum array for certain periods
    def sumParallelArrayDiscount(currentXAxis, dataDict)
        tempXAxis, tempYAxis = [], []
        tempYAxis2, tempYAxis3 = [], []
        prevXAxis, prevYAxis = [], []
        prevYAxis2 = []; prevYAxis3 = []

        arraySize = currentXAxis.size
        dataDict[:prevMonth].times do |i|
            month = i + 1
            if dataDict.has_key? :prodSer
                queryObj = {
                        :username => dataDict[:user],
                        "periods.prod_ser" => dataDict[:prodSer],
                        "periods.year" => dataDict[:prevYear].to_s,
                        "periods.month" => month.to_s,
                        "periods.week" => nil,
                }
            else
                queryObj = {
                        :username => dataDict[:user],
                        "periods.year" => dataDict[:prevYear].to_s,
                        "periods.month" => month.to_s,
                        "periods.week" => nil,
                }
            end # End of If condition to check if the key prodSer is there
            prevDoc = dataDict[:coll].find(queryObj)
            prevDoc.each do |doc|
                tempXAxis = doc[dataDict[:symbol]][:xAxis]
                tempYAxis = doc[dataDict[:symbol]][:yAxis]
                tempYAxis2 = doc[dataDict[:symbol]][:yAxis2]
                tempYAxis3 = doc[dataDict[:symbol]][:yAxis3]
            end # End of prevYearDoc iteration
            
            arrayTempSize = tempXAxis.size
            arraySize.times do |j|
                arrayTempSize.times do |k|
                    if currentXAxis[j] == tempXAxis[k]
                        prevXAxis[j] = currentXAxis[k]
                        if prevYAxis[j].nil?
                            prevYAxis2[j] = 0
                            prevYAxis3[j] = 0
                        end
                        prevYAxis2[j] += tempYAxis2[k]
                        prevYAxis3[j] += tempYAxis3[k]
                    end # End of if condition
                end # End of arrayTempSize
            end # End of arraySize
        end # End of Month iteration
        arraySize.times do |l|
            prevYAxis[l] = ScalarCalculators.calculateDiscount(prevYAxis2[l], prevYAxis3[l])
        end # End of arraySize iteration

        return prevXAxis, prevYAxis
    end # End of sumParallelArrayDiscount method

    # Sum array for certain periods
    def sumParallelArray(currentXAxis, dataDict)
        tempXAxis, tempYAxis = [], []
        prevXAxis, prevYAxis = [], []
        arraySize = currentXAxis.size
        dataDict[:prevMonth].times do |i|
            month = i + 1
            if dataDict.has_key? :prodSer
                queryObj = {
                        :username => dataDict[:user],
                        "periods.prod_ser" => dataDict[:prodSer],
                        "periods.year" => dataDict[:prevYear].to_s,
                        "periods.month" => month.to_s,
                        "periods.week" => nil,
                }
            else
                queryObj = {
                        :username => dataDict[:user],
                        "periods.year" => dataDict[:prevYear].to_s,
                        "periods.month" => month.to_s,
                        "periods.week" => nil,
                }
            end # End of If condition to check if the key prodSer is there
            prevDoc = dataDict[:coll].find(queryObj)
            prevDoc.each do |doc|
                tempXAxis = doc[dataDict[:symbol]][:xAxis]
                tempYAxis = doc[dataDict[:symbol]][:yAxis]
            end # End of prevYearDoc iteration
            
            arrayTempSize = tempXAxis.size
            arraySize.times do |j|
                arrayTempSize.times do |k|
                    if currentXAxis[j] == tempXAxis[k]
                        prevXAxis[j] = currentXAxis[k]
                        if prevYAxis[j].nil?
                            prevYAxis[j] = 0
                        end
                        prevYAxis[j] += tempYAxis[k]
                    end # End of if condition
                end # End of arrayTempSize
            end # End of arraySize
        end # End of Month iteration
        return prevXAxis, prevYAxis
    end # End of sumParallelArray method

end # End of Calculator module
