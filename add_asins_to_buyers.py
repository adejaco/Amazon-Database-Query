
  
import urllib2
import urllib
import pandas 
import csv
import time
import mechanize

from time import sleep
field_map = {
    "amazon-order-id" : "order_id",
    "purchase-date" : "purchase-date",

    "order-status" : "order-status",
    "ship-service-level" : "ship-level",
    "product-name" : "product-name",
    "asin" : "asin",
    "quantity" : "quantity",

    "item-price" : "price",
    "ship-city" : "city",
    "ship-state" : "state",
    "ship-postal-code" : "zipcode",
    "ship-country" : "country",

    "buyer_name":"buyer_name",
    "shipping_name":"shipping_name",
    "shipping_street":"shipping_street",
    "shipping_phone":"shipping_phone",
    "buyer_email":"buyer_email",
    "purchase-time" : "purchase-time",
    "currency" : "currency",
    "sales-channel":"sales-channel"
    
#carrier,purchase-time,fullfillment_id,purchase-date-int
    
 #   "multiple-asin" : "multiple-asin"
    
 
}
fields = field_map.keys()
fieldlist = [
    "order_id",
    "purchase-date",

    "order-status",
    "ship-level",
    "product-name",
    "asin",
     "quantity",

     "price",
    "city",
    "state",
   "zipcode",
   "country",

   "buyer_name",
   "shipping_name",
  "shipping_street",
  "shipping_phone",
  "buyer_email",
   "purchase-time",
    "currency",
   "sales-channel",
   "purchase-date-int",
   "fulfillment-id"
 
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
def try_amazon(url,wait,br):
   
     print "Try Amazon one more time",wait
     sleep(wait)
     try: 
         f = br.open(url)
         page = f.read()
         f.close()
#  print page
         startlink = page.find('Shipping Address')
         if startlink == -1: 
             if wait >= 8:
                 return -1  # amazon finally failed
             else:
                 return try_amazon(url,wait+2)
         else:
             return page
     except:
         return ""
    
def login_amazon_seller():#This function is just to return the webpage contents; the source of the webpage when a url is given.

 br1 = mechanize.Browser()  
 br1.set_handle_robots(False)  
 br1.addheaders = [("User-agent", "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.2.13) Gecko/20101206 Ubuntu/10.10 (maverick) Firefox/3.6.13")]  

 sign_in = br1.open('https://sellercentral.amazon.com/gp/sign-in.html')  

 br1.select_form(name="signinWidget")  
 br1["username"] = 'adejaco@yahoo.com' 
 br1["password"] = 'glutenfree1993'
 br1.method = "POST"
 logged_in = br1.submit() 
 return br1   


def get_page(url,br2): #This function is just to return the webpage contents; the source of the webpage when a url is given.

#br = mechanize.Browser()  


# br.set_handle_robots(False)   # no robots
# br.set_handle_refresh(False)  # can sometimes hang without this

 
 #br.addheaders = [("User-agent", "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.2.13) Gecko/20101206 Ubuntu/10.10 (maverick) Firefox/3.6.13")]  

 #sign_in = br.open('https://sellercentral.amazon.com/gp/sign-in.html')  

 #br.select_form(name="signinWidget")  
 #br["username"] = 'adejaco@yahoo.com' 
 #br["password"] = 'glutenfree1993'
 #br.method = "POST"
# logged_in = br.submit()    '''


 try:
#  print url
 
  f = br2.open(url)

#  f = urllib.urlopen(url)
  

  page = f.read()
  f.close()    #maybe we don't need to close this after every search
#  print page
  startlink = page.find('Shipping Address')
  if startlink == -1: # this means amazon won't return the page
 #try one more time
      return try_amazon(url, 2,br2)
               
  else:
      return page
 except: 
  return ""



'''
def get_page(url):
    try:
        req = urllib2.Request(url)
        response = urllib2.urlopen(req)          
        page = response.read()
#        req.close()
        
        startlink = page.find('To discuss automated access to Amazon data')
        if startlink != -1: # this means amazon won't return the page
            return -1
        else:
            return page
    except: 
        return ""
        '''


    
def get_buyer(page):
#    print page

    start_link = page.find('Shipping Address')
    if start_link == -1:
        print "can't find shipping address on page"
        return 
    start_name = page.find('<br>',start_link) + 4
    end_name = page.find('<',start_name)
    name = page[start_name:end_name]
    if len(name) > 100:
        name = -1
    
    start_street = page.find('>',end_name) + 1
    end_street = page.find('<',start_street)
    street = page[start_street:end_street]
    if len(street) > 100:
        street = -1
    
    start_phone = page.find('Phone:') + 6
    end_phone = page.find('<', start_phone + 3)
    phone = page[start_phone:end_phone]
   
 
        
    start_email = page.find('buyerEmail=') + 12
    end_email = page.find('@',start_email)
    email = page[start_email:end_email]
    if len(email) > 25:
        email = -1
    
    start_buyer = page.find('buyerID=')
    start_buyer1 = page.find('>',start_buyer)+1
    end_buyer = page.find('<',start_buyer1)
    buyer = page[start_buyer1:end_buyer]
    if len(buyer) > 200:
        buyer = -1
    
    return  name,street,phone,email,buyer
    
    
    
def get_asins(page):
#    print page

    start_link = page.find('http://www.amazon.com/gp/product/')
    if start_link == -1:
        print "can't find products on page"
        return 


    #create lists for multiple asins
    name = []
    quantity = []
    asin= []
    subtotal = []

    while page.find('http://www.amazon.com/gp/product/',start_link) != -1:
        start_link = page.find('http://www.amazon.com/gp/product/',start_link)
        start_product = page.find('>',start_link) + 1
        end_product = page.find('<',start_product)
    
        if end_product - start_product > 100:
            name.append(-1)
        else:
            name.append(page[start_product:end_product])

    
        start_quantity = page.find('Quantity:',start_link) 
        start_quantity = page.find('left">',start_quantity) +len('left">')
        end_quantity = page.find('</', start_quantity)
        if end_quantity - start_quantity > 100:
            quantity.append(-1)
        else:
            quantity.append(page[start_quantity:end_quantity])
   
   
        start_asin = page.find('ASIN:',start_link) 
        start_asin = page.find('left">',start_asin) +len('left">')
        end_asin = page.find('</', start_asin)
        if end_asin - start_asin > 20:
            asin.append(-1)
        else:
            asin.append(page[start_asin:end_asin])
            
        start_subtotal = page.find('Subtotal:',start_link) 
        start_subtotal = page.find('nowrap>',start_subtotal) +len('nowrap>')+1
        end_subtotal = page.find('</', start_subtotal)
        if end_subtotal - start_subtotal > 20:
            subtotal.append(-1)
        else:
            subtotal.append(page[start_subtotal:end_subtotal])   
         
# find fulfillment method
         
        start_fullfillment = page.find('Fulfillment method') 
        start_fullfillment = page.find('Seller',start_fullfillment,start_fullfillment + 100)
        if start_fullfillment != -1:
            fullfill = "Seller"
            break
        else:
            fullfill = "Amazon"
        #look for another item
            start_link +=100
    
    #stopped here ############################################
    return  name,quantity,asin,subtotal,fullfill   
'''
def union(p,q):
    for e in q:
        if e not in p:
            p.append(e)


def get_all_links(page):
    links = []
    while True:
        url,endpos = get_next_target(page)
        if url:
            links.append(url)
            page = page[endpos:]
        else:
            break
    return links

def crawl_web(seed,max_pages):
    tocrawl = [seed]
    crawled = []
    first_pages =0
    while tocrawl and first_pages < max_pages:
        page = tocrawl.pop() #page url has been removed from tocrawl list
#        print 'page =***',page
        if page not in crawled:
           
            crawled.append(page) 
            union(tocrawl,get_all_links(get_page(page))) #tocrawl contains all the new links
#            print 'tocrawl = *****', tocrawl
            first_pages = first_pages+1
       # print first_pages
#    print 'tocrawl = *****',tocrawl
    return    crawled,tocrawl
                                                         # from page
links= crawl_web('http://www.udacity.com/cs101x/index.html',500)  
'''    
    
# main program
# read in CSV files  
#time_of_day = time.strftime("%H:%M:%S") 
#date = time.strftime("%m/%d/%Y")
#print date+" "+time_of_day

input_data = csv.DictReader(open("buyers_without_asins_test.csv"))
# open up main database to get records
filename = "all_orders_with_asins.csv"
with open(filename, 'w') as master_file:
        master_file.close()
with open(filename, 'ab') as master_file:
    #build header
   
       for count,field1 in enumerate (fieldlist):
           if count == 0:
               header = field1 + ","
           elif count == len(fieldlist)-1:
               header = header + field1 +"\n"
           else:
               header = header + field1 + "," 
           
       master_file.write(header)
       writer_out= csv.writer(master_file,delimiter=',') 

#br = login_amazon_seller()
       count = 0
       br2 = login_amazon_seller()
       for order in input_data: # what is competitor index for
#shows buyer info from order
#http://sellercentral.amazon.com/gp/orders-v2/details?ie=UTF8&orderID=116-5884796-9313834
#shows past orders from buyer
#http://sellercentral.amazon.com/gp/orders-v2/list/ref=ag_myo_qlb1_myo?searchDateOption=noTimeLimit&showCancelled=0&searchKeyword=4s9x27h7z81nc8f%40marketplace.amazon.com&searchType=BuyerEmail&_encoding=UTF8
        if order['order-status'] == "Cancelled":
            continue
        url_order = "https://sellercentral.amazon.com/gp/orders-v2/details?ie=UTF8&orderID=" + order['order_id']       # get detailed page for the competitors product 
  
    #get buyer fields from this page
        page = get_page(url_order,br2)

        if page == "" or page == -1 :
            print "amazon denied access to order id = " + order['order_id'] 
    
            continue

# If order produces multiple asins then each asin gets a separate order

        name,quantity,asin,subtotal,order["fulfillment-id"]  = get_asins(page)
        
        if len(name) > 1:
            print "multiple asins in order " + order['order_id'] 
 

        for i in range(len(name)):
            order['product-name']=name[i]
            order['quantity'] =quantity[i]
            order['asin']=asin[i]
            order['price']=subtotal[i]
   
        
            lineout = []
            if i == 0: #first time through for order fix the fields
                order["currency"] = "USD"
                order["sales-channel"] = "Amazon.com"
                for field in fieldlist:
   # need to fix zipcodes, purchase_date, purchase_time, purchase_date_int               
                    if field in ["purchase-date"]:
                #search string for "T" which separates date from time and split into 2 fields
                        val = order[field]
                        split = val.find("T")
                        order[field]= val[5:7]+'/'+val[8:10] + '/'+ val[0:4]
                        order["purchase-time"]= val[split+1:split+9]
                        order["purchase-date-int"]= int(convert_date(order["purchase-date"]))
                    elif field in ["ship-postal-code"]:
                #make sure zip code are all xxxxx-yyy
                # nmake sure main zip is 5 digits
                        val = order[field]
                        while len(val) < 5:
                            val = "0" + val
                        if val.find("-") == -1 and len(val) <= 6:   # if extended zip isn't used add -0000
                                                            # don't extend the zip if it is canada
                            val = val + "-0000"
                        order[field] = val
              
              
                    lineout.append(order[field])
            else:
                for field in fieldlist:
                      lineout.append(order[field])
                
            writer_out.writerow(lineout) 
            count = count +1
            if count%100 == 0:
                print count
#    print name
 #   print stock_left,shipping
#    line = [date,time_of_day,name,company,size,ASIN,price,rank,prime,seller_review_count,stock_left,shipping,"Success"]
#    with open(master, 'ab') as master_file:  
#        writer_out= csv.writer(master_file,delimiter=',') 
#        writer_out.writerow(line) 
#        master_file.close()
   
print "Successfully completed"                                               
