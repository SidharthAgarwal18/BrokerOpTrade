from cmath import nan
from distutils.dir_util import copy_tree
import this
import pandas as pd
import re
# we want dates in YYYY/MM/HH

def MONTH2MM(curr_date):
    month = '01'
    if('jan' in curr_date.lower()):
        month = '01'
    elif('feb' in curr_date.lower()):
        month = '02'
    elif('mar' in curr_date.lower()):
        month = '03'
    elif('apr' in curr_date.lower()):
        month = '04'
    elif('may' in curr_date.lower()):
        month = '05'
    elif('jun' in curr_date.lower()):
        month = '06'
    elif('jul' in curr_date.lower()):
        month = '07'
    elif('aug' in curr_date.lower()):
        month = '08'
    elif('sep' in curr_date.lower()):
        month = '09'
    elif('oct' in curr_date.lower()):
        month = '10'
    elif('nov' in curr_date.lower()):
        month = '11'
    else:
        month = '12'
    return month


def dateDMY(curr_date):       # for format HH/MM/YYYY
    terms = re.split('/',curr_date)
    return terms[2] + '-' + terms[1] + '-' + terms[0]

def dateBern(curr_date):      # for format HH-MONTH
    yyyy = '2016'             # can be found from other files or today's date many options..

    terms = re.split('-',curr_date)
    dd = terms[0]

    mm = MONTH2MM(curr_date)

    return yyyy +'-'+mm +'-' + dd
    
def dateAddDash(this_date):
    this_date = str(this_date)
    return this_date[0:4] +'-' + this_date[4:6] +'-'+ this_date[6:8]

"""
class broker
    nameid
    allowed_brokers = []
    Borker(this_security,map_security,this_date,this_price,this_quent,this_beta,nameid):

        this.this_security = this_

        ...
    def returnDict(self):
        for idx in df.index:               # handle nans later
            # add statement to handle nan 
            curr_secur = security_map[map_security][df[this_security][idx]]
            this_dict[curr_secur] = []

            this_dict[curr_secur].append(fn1(df[this_date][idx],nameid))
            this_dict[curr_secur].append(fn2(df[this_price][idx],nameid))
            this_dict[curr_secur].append(fn3(df[this_quant][idx]))
            this_dict[curr_secur].append(fn4(df[this_beta][idx]))

    def fn1():
        if nameid is not in allowed_brokers:
            return error
        if nameid is == albaster:
            dateDMY

"""

def AlabasterData(df,security_map):
    this_security = "BB_ID"
    map_security = "BB_ID"
    this_date = "Date"
    this_price = "PriceUSD"
    this_quant = "Qty"
    this_beta = "Beta"
    #denote above as params

    # generatlDataCLeaner(params,"albaster")

    this_dict = {}

    for idx in df.index:     #dataframe of csv read
        if(df[this_security][idx]=='nan'):
            continue
        # df[this_security][idx]
        curr_secur = security_map[map_security][df[this_security][idx]] #
        this_dict[curr_secur] = []  
        #date,price,quantity,beta order 

        this_dict[curr_secur].append(dateDMY(df[this_date][idx])) 
        this_dict[curr_secur].append(df[this_price][idx])
        this_dict[curr_secur].append(df[this_quant][idx])
        this_dict[curr_secur].append(df[this_beta][idx])

    return this_dict

def BernSteinData(df,security_map):
    this_security = "GenericTicker"
    map_security = "GENERIC"
    this_date = "Date"
    this_price = "Price"
    this_quant = "Quantity"
    this_beta = "Beta"

    this_dict = {}

    for idx in df.index:               # handle nans later
        curr_secur = security_map[map_security][df[this_security][idx]]
        this_dict[curr_secur] = []

        this_dict[curr_secur].append(dateBern(df[this_date][idx]))
        this_dict[curr_secur].append(df[this_price][idx])
        this_dict[curr_secur].append(df[this_quant][idx])
        this_dict[curr_secur].append(df[this_beta][idx])

    return this_dict
    

def ComWealth(df,security_map):
    this_security = "RIC$Code"
    map_security = "RIC"
    this_date = "Trade$Date"
    this_price = "Executed$Price"
    this_quant = "Executed$Quantity"
    this_beta = "Beta$Hundreds"

    this_dict = {}

    for idx in df.index:               # handle nans later
        curr_secur = security_map[map_security][df[this_security][idx]]
        this_dict[curr_secur] = []

        this_dict[curr_secur].append(dateAddDash(df[this_date][idx]))
        this_dict[curr_secur].append(df[this_price][idx])
        this_dict[curr_secur].append(df[this_quant][idx])
        this_dict[curr_secur].append((df[this_beta][idx])/100)

    return this_dict


def returnBroker(inputFileName):                      # Takes the filename and returns the Broker name 
    if "alabaster" in inputFileName:
        return "alabaster"
    elif "bernstein" in inputFileName:
        return "bernstein"
    else:
        return "comwealth"

def fillSecurityMap(mappingFileName):
    list_of_securities = []
    security_map = {}                      
    df = pd.read_csv(mappingFileName)

    columns = list(df.columns)
    for column in columns:
        if column!="Security":
            security_map[column] = {}

    for idx in df.index:
        if df["Security"][idx] not in list_of_securities:         #optimize this loop
            list_of_securities.append(df["Security"][idx])

        for column in columns:    #columns = [Security,BB_ID,GENERIC,RIC]
            if (column!="Security"):
                #column = "RIC"
                #security_map[column][df[column][idx]] = "R.AAN00.RIC"
                # df["Security"][idx] = "AAN EQUITY"
                security_map[column][df[column][idx]] = df["Security"][idx]
    
    return security_map,list_of_securities

def fillCleanData(broker_dict,df,file_name,security_map): 
    broker_name = returnBroker(file_name)
    broker_dict[broker_name] = {}

    if(broker_name=="alabaster"):
        broker_dict[broker_name] = AlabasterData(df,security_map) #returns {Security:[date,price,quant,beta]}
    elif(broker_name=="bernstein"):
        broker_dict[broker_name] = BernSteinData(df,security_map) #returns {Security:[date,price,quant,beta]}
    else:
        broker_dict[broker_name] = ComWealth(df,security_map) #returns {Security:[date,price,quant,beta]}

if __name__=='__main__':

    totalFiles = 3

    inputFiles = {}
    inputFiles[0] = "input_alabaster/alabaster_20160101_tradelist.csv"
    inputFiles[1] = "input_bernstein/bernstein_TradeInputList_160101.csv"
    inputFiles[2] = "input_comwealth/trades_comwealth_jan2016.csv"

    list_of_brokers = []
    for input_name in inputFiles.values():
        list_of_brokers.append(returnBroker(input_name))

    mappingFileName = "mapping/MappingTable.csv"
    security_map,list_of_securities = fillSecurityMap(mappingFileName)
    
    broker_dict = {}   # broker_dict.keys() = ['alabster','bernstien',comwealth]

    for file_name in inputFiles.values():
        df = pd.read_csv(file_name)
        fillCleanData(broker_dict,df,file_name,security_map)

    default_price = -1
    default_quant = 0
    default_beta = -1
    default_date = "2016-01-01"

    #Code for operations file
    output_operations_name = "output-operations/output.csv"
    file = open(output_operations_name,"w")
    file.close()

    oper_dict = {"Security":[],"Date":[]}
    for broker_name in list_of_brokers:
        oper_dict["Price_"+broker_name] = []
        oper_dict["Quantity_"+broker_name] = []
        oper_dict["Beta_"+broker_name] =  []
    
    for security in list_of_securities: 
        this_test = False           # wheather prices are different
        this_date = default_date
        this_price = default_price

        for broker in list_of_brokers:
            if(broker_dict[broker].get(security) is None):
                continue
            if(broker_dict[broker][security][0]!=None): #date ignore this here
                this_date = broker_dict[broker][security][0]

            if(broker_dict[broker][security][1]!=None):
                if(this_price==default_price):    #entered this if statement for the first time..
                    this_price = broker_dict[broker][security][1]
                elif(this_price!=broker_dict[broker][security][1]):
                    this_test = True

        if(this_test):
            oper_dict["Security"].append(security)
            oper_dict["Date"].append(this_date)

            for broker in list_of_brokers:
                if(broker_dict[broker].get(security)!=None):
                    oper_dict["Price_"+broker].append(broker_dict[broker][security][1])
                    oper_dict["Quantity_"+broker].append(broker_dict[broker][security][2])
                    oper_dict["Beta_"+broker].append(broker_dict[broker][security][3])
                else:
                    oper_dict["Price_"+broker].append(default_price)
                    oper_dict["Quantity_"+broker].append(default_quant)
                    oper_dict["Beta_"+broker].append(default_beta)
        else:
            pass
    
    #for key in oper_dict.keys():
    #    print(key + " "+str(len(oper_dict[key])))
    operating_df = pd.DataFrame(oper_dict)
    operating_df.to_csv(output_operations_name,index=False)


    # Code for the trading file
    output_trade_name = "output-trading/output.csv"
    file = open(output_trade_name,"w")
    file.close()
    trading_dict = {"Security":[],"Date":[],"AveragePrice":[],"TotalQuantity":[],"MaxBeta":[]}

    for security in list_of_securities:

        date_test = 0
        for broker in list_of_brokers:
            if(broker_dict[broker].get(security) is None):
                continue
            if(broker_dict[broker][security][0]!=None):
                trading_dict["Date"].append(broker_dict[broker][security][0])
                date_test = 1
                break

        if(date_test==0):          
            trading_dict["Date"].append(default_date)
        trading_dict["Security"].append(security)

        sum_price = 0
        num_price = 0
        sum_quant = 0
        max_beta = default_beta               

        for broker in list_of_brokers:
            if(broker_dict[broker].get(security) is None):
                continue
            if(broker_dict[broker][security][1]!=None):
                sum_price += broker_dict[broker][security][1]
                num_price += 1
            if(broker_dict[broker][security][2]!=None):
                sum_quant += broker_dict[broker][security][2]
            if(broker_dict[broker][security][3]!=None):
                max_beta = max(max_beta,broker_dict[broker][security][3])
        
        if(num_price!=0):
            trading_dict["AveragePrice"].append(sum_price/num_price)
        else:
            trading_dict["AveragePrice"].append(default_price)
        trading_dict["TotalQuantity"].append(sum_quant)
        trading_dict["MaxBeta"].append(max_beta)
    
    
    trading_df = pd.DataFrame(trading_dict)
    trading_df.to_csv(output_trade_name,index=False)

