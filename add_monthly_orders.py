# -*- coding: utf-8 -*-
"""
Created on Wed May 27 19:06:22 2015

@author: adejaco
"""

#!/usr/bin/env python
"""
Write an aggregation query to answer this question:
the match operator pulls out the documents of the database that you are interested in

"""

field_locate = [

    "buyer_name",
  
    "shipping_street",
    "city",
    "state",
     "zipcode",
      "shipping_name"
   
   ] 

def convert_date(date):   # assumes date is in the form mm/dd/yyyy
    date_int = 0
    month_ptr = date.find('/')
    month = date[0:month_ptr]
    day_ptr = date.find('/',month_ptr+1)
    day = date[month_ptr+1:day_ptr]
    year = date[day_ptr+1:]   
#    print year,month,day
    
    date_int  = (int(year)-2000)*372+int(month)*31+int(day)
    return date_int

def remove_non_ascii(text):
    if isinstance(text, basestring) == False:
        return text
    string = ""
    for i in text:
        if ord(i) < 128:
            string = string + i
        else:
            string = string + " "
    return string
    
def save_csv_file_from_query(query_results, filename):
    
    # write a csv file delimited by pipes
    with open(filename, 'w') as master_file:
        master_file.close()
    with open(filename, 'ab') as master_file:
    #build header
   
       for count,field1 in enumerate (field_map.values()):
           if count == 0:
               header = field1 + ","
           elif count == len(field_map)-1:
               header = header + field1 +"\n"
           else:
               header = header + field1 + "," 
           
       master_file.write(header)
       writer_out= csv.writer(master_file,delimiter=',') 

       for docs in query_results:
           lineout = []
           for field1 in field_map.values():
                        
               try: 
                   
                   docs[field1] = remove_non_ascii(docs[field1])     
                   lineout.append(docs[field1])
        
               except: # that field didn't exist in those records
                         
                   lineout.append(None)
                    
               
           
           writer_out.writerow(lineout) 
           
 #              writer_out.writerow(lineout) 
    # YOUR CODE HERE
    return
def get_db(db_name):
    from pymongo import MongoClient
   
    client = MongoClient("mongodb://localhost:27017")
    db = client.examples
  
    return db

def make_pipeline_repeat(asin1,asin2,asin3,asin4):
    # complete the aggregation pipeline
  #  pipeline = [{"$project":{"user.time_zone":"$user.time_zone"}}]
  #                       "screen_name": "$user.screen_name"}}}]
#{"$group":{"_id":"$buyer_email",
#                               "count":{"$sum":1}}},
#                {"$sort":{"count":-1}}]
# expandex B00L4DFODU,B012833N6O,B00CXW12EQ

    pipeline = [{"$match" : {"asin": {"$in":[asin1,asin2,asin3,asin4]}}},
                {"$group":{"_id":"$buyer_email",#"buyer_name": "$buyer_name",
                           #"shipping_street":"$shipping_street",
                               "order_count":{"$sum":1},"orders_total":{"$sum":"$price"}}},
                {"$sort":{"order_count":-1}}]

 #               {"$project":{"buyer_email":"$amazon_orders.buyer_email",
 #                        "buyer_name": "$amazon_orders.buyer_name",
 #                        "shipping_street":"$amazon_orders.shipping_street",
 #                          "city":"$amazon_orders.city",
 #                          "state":"$amazon_orders.state",
   #                        "zipcode":"$amazon_orders.zipcode",
    #                     "shipping_name":"$amazon_orders.shipping_name",
     #                       "_id":"$_id"}},
                            
       
   #             {"$sort":{"followers":-1}},
    #            {"$limit":1 }]

    return pipeline
def make_pipeline_location(asin1,asin2,asin3,asin4):
    # complete the aggregation pipeline
  #  pipeline = [{"$project":{"user.time_zone":"$user.time_zone"}}]
  #                       "screen_name": "$user.screen_name"}}}]
#{"$group":{"_id":"$buyer_email",
#                               "count":{"$sum":1}}},
#                {"$sort":{"count":-1}}]
# expandex B00L4DFODU,B012833N6O,B00CXW12EQ
 





    pipeline = [{"$match" : {"asin": {"$in":[asin1,asin2,asin3,asin4]}}},
              

                {"$project":{
                         "buyer_name": "$buyer_name",
                        "shipping_street":"$shipping_street",
                           "city":"$city",
                           "state":"$state",
                           "zipcode":"$zipcode",
                        "shipping_name":"$shipping_name",
                           "_id":"$_id"}}]
                            
       
   #             {"$sort":{"followers":-1}},
    #            {"$limit":1 }]

    return pipeline
def aggregate(db, pipeline):
    result = db.amazon_orders.aggregate(pipeline)
    return result



# Start main program

if __name__ == '__main__':
   
    
    
    #get information from file on asins
    
    import pprint
    import csv
    #get repeat customers by asin
    
    products = {
    "Expandex": {"group":"Expandex", "asin1":"B012819SH4" , "asin2": "B012833N6O","asin3":"B00L4DFODU","asin4":"0"},
    "Breadmix":{"group":"BreadMix", "asin1":"B00DTV98BO" , "asin2": "B00DTV98BO","asin3":"B00EHO10TK","asin4": "B00EHO10R2"},
    "Xanthan": {"group":"Xanthan", "asin1":"B00CYMU3TA" , "asin2": "B00IZDIMCM","asin3":"0","asin4":"0"},
    "NA_Citrate": {"group":"Na_Citrate", "asin1":"B00D393SVS" , "asin2": "B00PKHAQDY","asin3":"0","asin4":"0"},
    "Milk": {"group":"Milk", "asin1":"B013P7XS62" , "asin2": "B00IYRDTTA","asin3":"0","asin4":"0"},
    "Eggs": {"group":"Eggs", "asin1":"B00IYTQKOO" , "asin2": "0","asin3":"0","asin4":"0"},
    "Potassium": {"group":"Potassium", "asin1":"B00UI9F01C" , "asin2": "0","asin3":"0","asin4":"0"},
    "Guar": {"group":"Guar", "asin1":"B00IZDIMG8" , "asin2": "0","asin3":"0","asin4":"0"},
    "Buttermilk": {"group":"Buttermilk", "asin1":"B00EQMJFAE" , "asin2": "0","asin3":"0","asin4":"0"}
    }
    
    asin_map = {
    "15 Oz Xanthan Gum Gluten Free" : "B00CYMU3TA",
    "8 Oz Gluten Free Xanthan Gum" : "B00IZDIMCM",
     "Gluten Free Xanthan Gum (8 ounce)" : "B00IZDIMCM",
    "Bulk Potassium Citrate Powder (300grams/10.6oz)":"B00UI9F01C",
    "Butter Powder (11.5 Oz): GMO Free and Produced in USA":"B014DPDTE6",
    "Buttermilk Powder (12 Oz): GMO Free and Produced in USA" : "B00EQMJFAE",
    "Expandex Modified Tapioca Starch Gluten Free (13.5 Oz)" : "B012833N6O",
    "Expandex Modified Tapioca Starch(13.5oz)) by Gluten Free You and Me LLC":"B012833N6O",
    "Expandex Modified Tapioca Starch(13.5oz))":"B012833N6O",
    "Expandex Modified Tapioca Starch(13.5oz)":"B012833N6O",
    "Expandex Modified Tapioca Starch Gluten Free (2.5 lb)" : "B00L4DFODU",
    "Expandex Modified Tapioca Starch Gluten Free (5 lb) by Gluten Free You and Me LLC" : "B012819SH4",
    "Expandex Modified Tapioca Starch Gluten Free (5 lb)":"B012819SH4",
    "Food Grade Non-GMO Sodium Citrate (8oz/227g)" : "B00PKHAQDY",
    "Food Grade Non-GMO Sodium Citrate (8oz/227g) For Molecular Gastronomy Recipes)" : "B00PKHAQDY",
    "Food Grade Non-GMO Sodium Citrate (8oz/227g) For Molecular Gastronomy Recipes":"B00PKHAQDY",
    "Food Grade Non-GMO Sodium Citrate(16oz/454g)" : "B00D393SVS",
    "(16oz/454g) Food Grade Non-GMO Sodium Citrate":"B00D393SVS",

    "Gluten Free Bread Mix (3 French White Mixes)" : "B00EHO10R2",
    "Gluten Free Bread Mix (3 Mix Flavor Pack)" : "B00DTV98BO",
    "Gluten Free Bread Mix (3 Mock Rye Mixes)" : "B00EHO10TK",
    "Potato Starch (2.5 lb) Gluten Free" : "B00D39204O",
    "Gluten Free Bread Mix (3 Italian Herb Mixes)" : "B00DTV98BO",
    "Rooibos Loose Leaf Tea (8 oz): Red" : "B00IYTQK9O",
    "Rooibos Loose Leaf Tea (8 oz): Red, Long Cut, Organic Ingredients":"B00IYTQK9O",
    "Whole Milk Powder (12 Oz): Non-GMO and Produced in USA" : "B00IYRDTTA",
    "Whole Milk Powder (11.5 Oz): Non-GMO and Produced in USA":"B00IYRDTTA",
    "Whole Milk Powder (24 Oz/1.5lb/680 grams): Non-GMO and Produced in USA" : "B013P7XS62",
    "Whole Milk Powder (24 Oz/ 1.5lb): Non-GMO and Produced in USA": "B013P7XS62",
    "1/2lb (8oz) Dried Egg Whites (Non-GMO & Pasteurized)":"B00IYTQKOO",
    "12 Oz Guar Gum Gluten Free":"B00IZDIMG8",
    "Cheddar Cheese Powder (12 Oz): Non-GMO;Produced in USA; Delicious Flavor with Better Ingredients":"B00EPLPEM4",
    "Butter Powder (24Oz/1.5lb/680grams): GMO Free and Produced in USA":"B014DPDTE6",
    "Citric Acid Powder (11.5 Oz): Natural Food Preservation as well as a Dishwasher Detergent":"B00IYSUOZG",
    "White Rice Flour 2.5 lbs (Gluten Free Baking Ingredients)":"B00EPLPEM4"
    
     }
     
    field_map = {
    "amazon-order-id" : "order_id",
    "purchase-date" : "purchase-date",
    "purchase-date-int":"purchase-date-int",
    "sales-channel":"sales-channel",
    "order-status" : "order-status",
    "ship-service-level" : "ship-level",
    "product-name" : "product-name",
    "asin" : "asin",
    "quantity-shipped" : "quantity",
    "currency" : "currency",
    "shipping-price" : "shipping-price",
    "item-price" : "price",
    "ship-city" : "city",
    "ship-state" : "state",
    "ship-postal-code" : "zipcode",
    "ship-country" : "country",
    "purchase-time" : "purchase-time",
    "buyer-name":"buyer_name",
    "recipient-name":"shipping_name",
    "shipping-address-1":"shipping_street",
    "buyer-phone-number":"shipping_phone",
    "buyer-email":"buyer_email",
    "shipment-item-id":"shipment-id",
    "fulfillment-center-id":"fulfillment-id",
    "carrier":"carrier"
    
     }

     
    fields = field_map.keys()
    
    
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    db = client.examples
    print db.amazon_orders.count()   
    
    input_data = csv.DictReader(open("Feb-12-2016.csv"))
    
    for document in input_data: #go through every new order
    
    #check that we have a unique shipment_id that isn't in the database
        query1 = {"shipment-id":document["shipment-item-id"]} 
        duplicate = db.amazon_orders.find_one(query1)
        if duplicate != None and document["shipment-item-id"] != "none" :
            print "shipment_id matches ", document["shipment-item-id"]
            
            continue      #skip this duplicate order
        #clean desired fields 
        #email: strip off after @
        #zip code: insure XXXXX-YYYY
     
        order = {}
 
        for field, val in document.iteritems():
            if field not in fields: #if not one of the fields we want skip it
                continue
          # we need to clean up zip code such that it is xxxxx-yyy
          # and we need to split date and time into 2 fields
          # strip off email suffix

            if field in ["purchase-date"]:
                #search string for "T" which separates date from time and split into 2 fields
                split = val.find("T")
                order[field_map[field]] = order[field_map[field]] = val[5:7]+'/'+val[8:10] + '/'+ val[0:4]
                order["purchase-time"]= val[split+1:split+9]
            
            
            
            
            elif field in ["ship-postal-code"]:
                #make sure zip code are all xxxxx-yyy
                # nmake sure main zip is 5 digits
                while len(val) < 5:
                    val = "0" + val
                if val.find("-") == -1 and len(val) <= 6:   # if extended zip isn't used add -0000
                                                            # don't extend the zip if it is canada
                    val = val + "-0000"
                order[field_map[field]] = val
            elif field in ["buyer-email"]:
                at = val.find("@")
                order[field_map[field]]= val[0:at]
        
            elif field in ['item-price']:
                order[field_map[field]] = float(val)
            elif field in ['quantity-shipped']:
                order[field_map[field]] = int(val)   
            else:
                order[field_map[field]] = remove_non_ascii(val)
        
        order["asin"] = asin_map[order["product-name"]]  
        order["purchase-date-int"]= int(convert_date(order["purchase-date"]))     
        #now check if the email matches a previous order in the database
        db.amazon_orders.insert(order)

    print db.amazon_orders.count() 
    
    all_orders = db.amazon_orders.find()
    save_csv_file_from_query(all_orders, 'all_orders.csv')    
    
    print "successful"   
       
           
          
    # YOUR CODE HERE