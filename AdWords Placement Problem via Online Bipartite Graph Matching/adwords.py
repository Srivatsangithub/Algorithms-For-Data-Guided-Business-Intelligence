import pandas as pd
import random
import sys
import math

random.seed(0)

# Note: Initial method was executing the entire program using the DataFrame and a couple of temp series. However, execution time was found to be very large especially due
# to the usage of iloc and loc for indexing and hence dictionaries with a mix of dataframes had to be used to improve performance drastially
# Time taken for Greedy Algorithm to run one iteration was approximately 2 mins rendering it inefficient to run the program for almost 3 hours 10 minutes to calculate the 
# competetive ratio.

# The path to the data folder should be given as input
if len(sys.argv) != 2:
    print('adwrods.py {greedy/balance/msvv}')
    sys.exit(1)
algorithm_type = sys.argv[1]

# Reading the queries from the file
file_name = open('./queries.txt', 'r')
queries = file_name.readlines()
for i in range(0, len(queries)):
    queries[i] = queries[i].strip()

bidder_df = pd.read_csv('./bidder_dataset.csv')
#bidder_df.head(10)

# To store the key value pairs of each Advertiser and its budget in a df itself
advertiser_budget_df = bidder_df.filter(['Advertiser', 'Budget'], axis=1)
advertiser_budget_df.dropna(inplace = True)
#advertiser_budget_df.head(20)

# Global value that is required for msvv
advertiser_budget_dict_global = advertiser_budget_df.set_index('Advertiser').T.to_dict()

def greedy_algo(queries):
    # Converted the advertiser and budget dataframe to a dictionary of key value pairs.
    advertiser_budget_dict = advertiser_budget_df.set_index('Advertiser').T.to_dict()

    # Converting df to a dictionary of keyword: {Aid: [Bid Value]}
    query_ad_bid_dict = {k: f.groupby('Advertiser')['Bid Value'].apply(list).to_dict() for k, f in bidder_df.groupby('Keyword')}
    #query_ad_bid_dict

    greedy_revenue = 0.0
    for key in queries:
        count = 0
        # To check if every Advertiser's ID has a budget more than 0
        for id in query_ad_bid_dict[key]:
            if advertiser_budget_dict[id]['Budget'] <= 0:
                count+=1
        # To skip the query if all the budgets are 0
        if count == len(query_ad_bid_dict[key]):
            continue
        else:
            # To check if every Advertiser's max bid is less than the budger
            max_bid_val = 0.0
            id_max_bid_val = 0
            for id in query_ad_bid_dict[key]:
                id_bid_val = id
                bid_val = query_ad_bid_dict[key][id][0] 
                id_budget = advertiser_budget_dict[id]['Budget']
                # A check is made to see if bid value is the max value, if there is enough budget, and if smaller id is chosen for equal max bids
                if (bid_val > max_bid_val and bid_val <= id_budget):
                    max_bid_val = bid_val
                    id_max_bid_val = id_bid_val
            if max_bid_val == 0:
                continue
            greedy_revenue+=max_bid_val
            advertiser_budget_dict[id_max_bid_val]['Budget']-=max_bid_val
    return greedy_revenue

def msvv_algo(queries):
    # Converted the advertiser and budget dataframe to a dictionary of key value pairs.
    advertiser_budget_dict = advertiser_budget_df.set_index('Advertiser').T.to_dict()

    # Converting df to a dictionary of keyword: {Aid: [Bid Value]}
    query_ad_bid_dict = {k: f.groupby('Advertiser')['Bid Value'].apply(list).to_dict() for k, f in bidder_df.groupby('Keyword')}
    #query_ad_bid_dict

    msvv_revenue = 0.0
    for key in queries:
        count = 0
        # To check if every Advertiser's ID has a budget more than 0
        for id in query_ad_bid_dict[key]:
            if advertiser_budget_dict[id]['Budget'] <= 0:
                count+=1
        # To skip the query if all the budgets are 0
        if count == len(query_ad_bid_dict[key]):
            continue
        else:
            # If budgets are legal, finding the max product of function and fraction and using that bid for revenue
            max_bid_val = 0.0
            id_max_bid_val = 0
            max_product = 0

            for id in query_ad_bid_dict[key]:
                id_bid_val = id
                bid_val = query_ad_bid_dict[key][id][0] 
                id_budget = advertiser_budget_dict[id]['Budget']
                original_budget = advertiser_budget_dict_global[id]['Budget'] 
                xu_fraction = (original_budget - id_budget)/original_budget
                func_xu_fraction = 1 - math.exp((xu_fraction - 1))
                # A check is made to see if product is the max value, if there is enough budget, and if smaller id is chosen for equal product values
                if (bid_val*func_xu_fraction >= max_product and bid_val <= id_budget):
                    if (bid_val*func_xu_fraction > max_product or id_bid_val < id_max_bid_val):
                        max_bid_val = bid_val
                        id_max_bid_val = id_bid_val
                        max_product = bid_val*func_xu_fraction
            if max_bid_val == 0:
                continue
            msvv_revenue+=max_bid_val
            advertiser_budget_dict[id_max_bid_val]['Budget']-=max_bid_val
    return msvv_revenue

def balance_algo(queries):
    # Converted the advertiser and budget dataframe to a dictionary of key value pairs.
    advertiser_budget_dict = advertiser_budget_df.set_index('Advertiser').T.to_dict()

    # Converting df to a dictionary of keyword: {Aid: [Bid Value]}
    query_ad_bid_dict = {k: f.groupby('Advertiser')['Bid Value'].apply(list).to_dict() for k, f in bidder_df.groupby('Keyword')}
    #query_ad_bid_dict

    balance_revenue = 0.0
    for key in queries:
        count = 0
        # To check if every Advertiser's ID has a budget more than 0
        for id in query_ad_bid_dict[key]:
            if advertiser_budget_dict[id]['Budget'] <= 0:
                count+=1
        # To skip the query if all the budgets are 0
        if count == len(query_ad_bid_dict[key]):
            continue
        else:
            # To use the bid value of the ID that has the maximum budget
            max_bid_val = 0.0
            id_max_bid_val = 0
            max_budget = 0
            for id in query_ad_bid_dict[key]:
                id_bid_val = id
                bid_val = query_ad_bid_dict[key][id][0] 
                id_budget = advertiser_budget_dict[id]['Budget']
                # A check is made to see if budget is the max value, if there is enough budget, and if smaller id is chosen for equal max budgets
                if (id_budget >= max_budget and bid_val <= id_budget):
                    if (id_budget > max_budget or id_bid_val < id_max_bid_val):
                        max_bid_val = bid_val
                        id_max_bid_val = id_bid_val
                        max_budget = id_budget
            if max_bid_val == 0:
                continue
            balance_revenue+=max_bid_val
            advertiser_budget_dict[id_max_bid_val]['Budget']-=max_bid_val
    return balance_revenue

def competetive_ratio_calculation():

    ALG = 0.0
    OPT = advertiser_budget_df['Budget'].sum()
    total_rev = 0.0
    # GREEDY ALGORITHM
    if algorithm_type == 'greedy':
        for i in range(0, 100):
            # shuffling queries to get hundred different inputs
            random.shuffle(queries)
            itr_revenue = greedy_algo(queries)
            total_rev+=itr_revenue
            #print(total_rev)
        ALG = total_rev/100
        competetive_ratio = ALG/OPT
        #print("Competetive Ratio :", competetive_ratio)         
        return competetive_ratio
    # BALANCE ALGORITHM
    elif algorithm_type == 'balance':
        for i in range(0, 100):
            # shuffling queries to get hundred different inputs
            random.shuffle(queries)
            itr_revenue = balance_algo(queries)
            total_rev+=itr_revenue
            #print(total_rev)
        ALG = total_rev/100
        competetive_ratio = ALG/OPT
        #print("Competetive Ratio :", competetive_ratio)         
        return competetive_ratio
    # MSVV ALGORITHM
    elif algorithm_type == 'msvv':
        for i in range(0, 100):
            # shuffling queries to get hundred different inputs
            random.shuffle(queries)
            itr_revenue = msvv_algo(queries)
            total_rev+=itr_revenue
            #print(total_rev)
        ALG = total_rev/100
        competetive_ratio = ALG/OPT
        #print("Competetive Ratio :", competetive_ratio)         
        return competetive_ratio

    
if algorithm_type == 'greedy':
   revenue = greedy_algo(queries)
   print('Revenue using Greedy Algorithm: ' , "{:.2f}".format(revenue))
   competetive_rat = competetive_ratio_calculation()
   print('Competetive Ratio for Greedy Algorithm: ', "{:.2f}".format(competetive_rat))
   
elif algorithm_type == 'msvv':
    revenue = msvv_algo(queries)
    print('Revenue using MSVV Algorithm: ' , "{:.2f}".format(revenue))
    competetive_rat = competetive_ratio_calculation()
    print('Competetive Ratio for MSVV Algorithm: ', "{:.2f}".format(competetive_rat))


elif algorithm_type == 'balance':
    revenue = balance_algo(queries)
    print('Revenue using Balance Algorithm: ' , "{:.2f}".format(revenue))
    competetive_rat = competetive_ratio_calculation()
    print('Competetive Ratio for Balance Algorithm: ', "{:.2f}".format(competetive_rat))

else:
    print("Invalid input, please enter the algorithm name")
    exit()




