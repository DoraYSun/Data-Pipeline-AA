#%%
from selenium import webdriver
from threading import Thread
from lxml import etree
import requests

class Car:

    def __init__(self):
        options = webdriver.ChromeOptions()
        # improve efficiency by setting browser to open without images 
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        #claim Google Chrome as browser
        self.driver = webdriver.Chrome()


    def car_detail(self, item):
        """get detailed info for each car"""  
        #use requests rather than drivers to save image loading 
        req = requests.get(item['URL'], headers=None)
        # generate xpath info for the page
        html = etree.HTML(req .text)
        #scraping car details by xpath
        try:
            # licence plate
            # find licence plate through alternative xpath to eliminate empty list
            if len(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[2]/article[2]/p/strong/text()')) == 0:
                item['license_plate'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[2]/article[3]/p/strong/text()')
            else:
                item['license_plate'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[2]/article[2]/p/strong/text()')
            # car make
            item['make'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[1]/div/div[1]/h1/span[1]/text()')
            # car model of the make
            item['model'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[1]/div/div[1]/h1/span[2]/text()')
            # car price
            item['price'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/div[1]/div/div/strong/text()') 
            # mileage droven
            item['mileage'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[1]/span[2]/span/text()')
            # year made of the car
            item['year'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[2]/span[2]/span/text()')
            # fuel type of the car
            item['fuel_type'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[3]/span[2]/span/text()')
            # transmission of the car
            item['transmission'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[4]/span[2]/span/text()')
            # body type of the car
            item['body_type'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[5]/span[2]/span/text()')
            # main colour of the car
            item['colour'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[6]/span[2]/span/text()')
            # door umbers
            item['doors'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[7]/span[2]/span/text()')
            # engine size of the car
            item['engine_size'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[8]/span[2]/span/text()')
            # co2 emissions of the car
            item['co2_emissions'] = html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[9]/span[2]/span/text()')
        except Exception as e:
            print('>>>>> unable to find elements', e)
           
        
    def car_list(self, item):
        """Obtain individual car URL from each main page""" 
        # find how many car info avaliable 
        car_numbers = self.driver.find_element_by_xpath('//*[@id="vl-list-container"]/div[4]/div/div[1]/strong[3]').text.replace(',', '')

        # determain the range of the page
        if int(car_numbers) % 20 == 0:
            page_range = int(car_numbers) // 20
        else:
            page_range = int(car_numbers) // 20 + 1

        # access to each page
        for page in range(1, page_range + 1):
            self.driver.get(f'https://www.theaa.com/used-cars/displaycars?sortby=datedesc&page={page}&pricefrom=0&priceto=1000000')  
            
            # obtain url for each car
            car_ls = self.driver.find_elements_by_xpath('//div [@class="finance-and-view"]')
            
            # generate car url list
            car_url_ls = [car.find_element_by_xpath('.//a').get_attribute('href') for car in car_ls]
            # define a task list
            task_ls = []   

            for url in car_url_ls:
                item['URL'] = url
                # handover car detail scraping task to threads
                task = Thread(target=self.car_detail, args=(dict(item),))
                task.start()    # start task
                task_ls.append(task)    # append task to task list

            # waiting for last task to finish in order to strat a new task
            for task in task_ls:
                 # finish waiting 
                 task.join() 
      
    
    def run(self):
        """accept cookies and initiate a dictionary for storing car info"""
        # access the initial page to accept cookies
        self.driver.get('https://www.theaa.com/used-cars/displaycars?sortby=datedesc&pricefrom=0&priceto=1000000') 
        # accept cookies 
        cookies = self.driver.find_element_by_xpath('//*[@id="truste-consent-button"]')
        cookies.click()

        # initiate a dictionary to store car info 
        item = {}  
        # pass item to next fuction as dictionaty
        self.car_list(dict(item)) 

        
    def __del__(self):
        # shut down the browser
        self.driver.close()  
        print('>>>>[Well Done]')


# test Car()
if __name__ == '__main__':
    test = Car()
    test.run()

