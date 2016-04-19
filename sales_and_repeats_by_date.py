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
   
       for count,field1 in enumerate (field_locate):
           if count == 0:
               header = field1 + ","
           elif count == len(field_locate)-1:
               header = header + field1 +"\n"
           else:
               header = header + field1 + "," 
           
       master_file.write(header)
       writer_out= csv.writer(master_file,delimiter=',') 

       for docs in query_results:
           lineout = []
           for field1 in field_locate:
               try: 
                   
                   docs[field1] = remove_non_ascii(docs[field1])     
                   lineout.append(docs[field1])
        
               except: # that field didn't exist in those records
                         
                   lineout.append(None)
                                   
           
           writer_out.writerow(lineout) 
    # YOUR CODE HERE
    return
def get_db(db_name):
    from pymongo import MongoClient
   
    client = MongoClient("mongodb://localhost:27017")
    db = client.examples
  
    return db
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
def make_pipeline_orders_by_asin_and_date(asin1,asin2,asin3,asin4,old,new):
    # complete the aggregation pipeline
  #  pipeline = [{"$project":{"user.time_zone":"$user.time_zone"}}]
  #                       "screen_name": "$user.screen_name"}}}]
#{"$group":{"_id":"$buyer_email",
#                               "count":{"$sum":1}}},
#                {"$sort":{"count":-1}}]
# expandex B00L4DFODU,B012833N6O,B00CXW12EQ




    pipeline = [{"$match" : {"$and": [
                                    {"asin": {"$in":[asin1,asin2,asin3,asin4]}},
                                    {"purchase-date-int":{"$lte":convert_date(new)}},
                                    {"purchase-date-int":{"$gte":convert_date(old)}}
                                   ] 
                            }},
                 {"$sort":{"order_id":-1}}          
                ] 
            #    ,
             #   {"$group":{"_id":"$buyer_email",#"buyer_name": "$buyer_name",
                           #"shipping_street":"$shipping_street",
              #                 "order_count":{"$sum":1},"orders_total":{"$sum":"$price"}}},
              #  {"$sort":{"order_count":-1}}]

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
def make_pipeline_quantity(order,asin1,asin2,asin3,asin4,old,new):
    # complete the aggregation pipeline
  #  pipeline = [{"$project":{"user.time_zone":"$user.time_zone"}}]
  #                       "screen_name": "$user.screen_name"}}}]
#{"$group":{"_id":"$buyer_email",
#                               "count":{"$sum":1}}},
#                {"$sort":{"count":-1}}]
# expandex B00L4DFODU,B012833N6O,B00CXW12EQ
 

# pipeline = [{"$match" : {"asin": {"$in":[asin1,asin2,asin3,asin4]}}},
 #               {"$group":{"_id":"$buyer_email",#"buyer_name": "$buyer_name",
                           #"shipping_street":"$shipping_street",
  #                             "order_count":{"$sum":1},"orders_total":{"$sum":"$price"}}},
   #             {"$sort":{"order_count":-1}}]



    pipeline = [{"$match" : {"$and": [
                                    {"order_id": {"$in":[order]}},
                                    {"asin": {"$in":[asin1,asin2,asin3,asin4]}},
                                    {"purchase-date-int":{"$lte":convert_date(new)}},
                                    {"purchase-date-int":{"$gte":convert_date(old)}}
                                   ] 
                            }},
              
              {"$group":{"_id":"$order_id",
                               "quantity_count":{"$sum":"$quantity"},"orders_total":{"$sum":"$price"}}}]

    return pipeline
def aggregate(db, pipeline):
    result = db.amazon_orders.aggregate(pipeline)
    return result

if __name__ == '__main__':
    db = get_db('Null')
    
    field_buyer = [

    "buyer_name",
    "shipping_name",
    "shipping_street",
    "city",
    "state",
     "zipcode",
    "shipping_phone",
    "buyer_email",
    "orders",
    "sales"
  
   ] 

    
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
    "Buttermilk": {"group":"Buttermilk", "asin1":"B00EQMJFAE" , "asin2": "0","asin3":"0","asin4":"0"},
    "Potato": {"group":"Potato", "asin1":"B00D39204O" , "asin2": "0","asin3":"0","asin4":"0"},
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
    "ship-address-1":"shipping_street",
    "buyer-phone-number":"shipping_phone",
    "buyer-email":"buyer_email",
    "shipment-item-id":"shipment-id",
    "fulfillment-center-id":"fulfillment-id",
    "carrier":"carrier"
    
     }
    fields = field_map.keys()
    
    
    
    old_date = "1/6/2016"
    new_date = "2/6/2016"
    
    total_all = 0
    total_sales_all =0  
    repeat_all = 0
    repeat_sales_all = 0
    bulk_count_all = 0
    bulk_sales_all = 0
    order_cost_reduction = 0
    
    for product_group in products:
        group = products[product_group]["group"]
        asin1 = products[product_group]["asin1"]
        asin2 = products[product_group]["asin2"]
        asin3 = products[product_group]["asin3"]
        asin4 = products[product_group]["asin4"]
 
    
    
    
        pipeline = make_pipeline_orders_by_asin_and_date(asin1,asin2,asin3,asin4,old_date,new_date)
        results = aggregate(db, pipeline)  #result have email numbers
   
        repeat = 0
        total = 0
        total_sales = 0
        repeat_sales = 0
        bulk_count = 0
        bulk_sales = 0
        last_order_id = None
        for document in results:
            total = total + document['quantity']
            total_sales = total_sales + document['price']
            #has this buyer purchased on an earlier date   
            query1 = {"$and": [
                                    {"buyer_email": document['buyer_email']},
                                    {"purchase-date-int":{"$lt":int(document['purchase-date-int'])}}
                     
                              ] 
                      }
            repeat_orders = db.amazon_orders.find_one(query1)
            
            
              
            
            if repeat_orders != None:
                repeat = repeat + document['quantity']
                repeat_sales = repeat_sales + document['price']
                
          #does this order have more than 1 product with it
            if document['order_id'] != last_order_id:
                last_order_id = document['order_id']
                pipeline = make_pipeline_quantity(document['order_id'],asin1,asin2,asin3,asin4,old_date,new_date)
                results_orders = aggregate(db, pipeline) 
                for order_count in results_orders:
                    if order_count["quantity_count"] > 1:
                        bulk_count += order_count["quantity_count"]
                        bulk_sales += order_count["orders_total"]
                        
          #how many products are in the bulk order              
                        
                        
                
        total_all += total
        total_sales_all += total_sales
        repeat_all += repeat
        repeat_sales_all += repeat_sales
        bulk_count_all += bulk_count
        bulk_sales_all += bulk_sales
        print "**********************"
        print "dates from ", old_date, " to ", new_date
        print "stats for ",group
        print "total item are",total
 #       print "repeat order items  are ",repeat
 #       print "bulk ordered items  are ",bulk_count
 #       if total != 0:
 #           print "% repeat items =", 100.*repeat/total
 #           print "% bulk items =", 100.*bulk_count/total
        print "total order sales $ are",total_sales
 #       print "repeat order sales $  are ",repeat_sales 
  #      print "bulk order sales $  are ",bulk_sales
        if total_sales != 0:
            print "% repeat sales =", 100.*repeat_sales/total_sales
            print "% bulk sales =", 100.*bulk_sales/total_sales
    
    #
    #now put the email list of repeat buyers in a csv file 
    #
    print "************************************"
    print "************************************"
    print "for all tracked asins over the dates "+old_date+" to " +new_date
  
    print "total items are",total_all
 #   print "repeat items  are ",repeat_all
 #   print "bulk items  are ",bulk_count_all
 #   print "% repeat items =", 100.*repeat_all/total_all
 #   print "% bulk items =", 100.*bulk_count_all/total_all
    print "total order sales $ are",total_sales_all
 #   print "repeat order sales $  are ",repeat_sales_all
 #   print "bulk order sales $  are ",bulk_sales_all  
    print "% repeat sales =", 100.*repeat_sales_all/total_sales_all
    print "% bulk sales =", 100.*bulk_sales_all/total_sales_all
    print "successful"   
       
           
          
    # YOUR CODE HERE