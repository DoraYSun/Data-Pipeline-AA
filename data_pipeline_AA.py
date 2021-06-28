from selenium import webdriver
from threading import Thread
from lxml import etree
import requests
import pandas as pd
import numpy as np
import time
import urllib.request
import boto3


class Car:

    def __init__(self):
        """set up driver and create a list for data saving"""
        options = webdriver.ChromeOptions()
        # improve efficiency by setting browser to open without images 
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        #claim Google Chrome as browser
        self.driver = webdriver.Chrome(chrome_options=options)
        self.car_details_list = []
    

    def car_detail(self, item):
        """get detailed info for each car""" 
        
        car_info_df = pd.read_csv('Output Test.csv')
        if item['URL'] in car_info_df.values:
            print('>>> car info already in database')
            return

        # obtain image url for each car
        for i in range(43, len(item['URL'])):
            if item['URL'][i] == '-':
                partial_img_url= item['URL'][(i+1):]
        item['image_url'] = 'https://image.vcars.co.uk/vcarsdna/' + partial_img_url + '_1.jpg'
        
        #use requests rather than drivers to save image loading 
        time.sleep(0.5)
        req = requests.get(item['URL'], headers=None) 
        
        # generate xpath info for the page
        html = etree.HTML(req.text)
        
        #scraping car details by xpath
        try:
            # licence plate
            # find licence plate through alternative xpath to eliminate empty list
            if len(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[2]/article[2]/p/strong/text()')) == 0:
                item['license_plate'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[2]/article[3]/p/strong/text()')[0])
            else:
                item['license_plate'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[2]/article[2]/p/strong/text()')[0])

            # car price
            item['price'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/div[1]/div/div/strong/text()')[0])
            # car make
            item['make'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[1]/div/div[1]/h1/span[1]/text()')[0])
            # car model of the make
            item['model'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[1]/div/div[1]/h1/span[2]/text()')[0])           
            # mileage droven
            item['mileage'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[1]/span[2]/span/text()')[0])
            # year made of the car
            item['year'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[2]/span[2]/span/text()')[0])
            # fuel type of the car
            item['fuel_type'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[3]/span[2]/span/text()')[0])
            # transmission of the car
            item['transmission'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[4]/span[2]/span/text()')[0])
            # body type of the car
            item['body_type'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[5]/span[2]/span/text()')[0])
            # main colour of the car
            item['colour'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[6]/span[2]/span/text()')[0])
            # door umbers
            item['doors'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[7]/span[2]/span/text()')[0])
            # engine size of the car
            item['engine_size'] = str(html.xpath('//*[@id="header"]/div[4]/main/div[3]/div[1]/section[3]/div/ul/li[8]/span[2]/span/text()')[0])
            
        except Exception as e:
            print('>>>>> unable to find elements', e)
        
        # download car images to local directory
        urllib.request.urlretrieve(item['image_url'], item['license_plate'])

        # upload car images to s3
        file_name = '/Users/dorasun/Desktop/Data-Pipeline/' + item['license_plate']
        object_name = item['license_plate'] +'.jpg'
        s3_client = boto3.client('s3')
        s3_client.upload_file(file_name, 'aaimgbucket', object_name)
        
        self.car_details_list.append(item)

    
    def car_list(self, item):
        """Obtain individual car URL from each main page""" 
        # find how many car info avaliable 
        car_numbers = self.driver.find_element_by_xpath('//*[@id="vl-list-container"]/div[4]/div/div[1]/strong[3]').text.replace(',', '')

        # determain the range of the page
        if int(car_numbers) % 20 == 0:
            page_range = int(car_numbers) // 20
        else:
            page_range = int(car_numbers) // 20 + 1
        
        for page in range(1, page_range):
            self.driver.get(f'https://www.theaa.com/used-cars/displaycars?sortby=datedesc&page={page}&pricefrom=0&priceto=1000000')
           
            # stop scraping when no info avalible
            if len(self.driver.find_elements_by_xpath('//div [@class="finance-and-view"]')) == 0:
                print('Information not avaliable')
                break
                
            else: 
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
                print(f'>> page{page}--[Done]')


    def run(self):
        """accept cookies and initiate a dictionary for storing car info"""
        # access the initial page to accept cookies
        self.driver.get('https://www.theaa.com/used-cars/displaycars?sortby=datedesc&pricefrom=0&priceto=1000000') 
        # accept cookies 
        cookies = self.driver.find_element_by_xpath('//*[@id="truste-consent-button"]')
        cookies.click()

        # initiate a dictionary to store car info 
        item = {}  
        # pass item to next fuction
        self.car_list(dict(item)) 

    
    def create_df(self):
        """save car details in a dataframe"""
        df = pd.DataFrame(self.car_details_list)
        df.to_csv('Output Test.csv', mode='a', encoding='utf8', header=None)


    def clean_df(self):
        """clean data output"""
        # clean duplicates and drop missing values 
        car_df = pd.read_csv('Output Test.csv', index_col=0, skip_blank_lines=True)
        car_df.reset_index(drop=True, inplace=True)
        car_df.replace('license_plate', np.nan, inplace=True)
        car_df.dropna(subset=['license_plate'], inplace=True)
        car_df.drop_duplicates(subset=['license_plate'],inplace=True)
        # clean 'price' column 
        car_df['price'] = car_df['price'].str.strip('£').str.replace(',', '')
        car_df['price'] = car_df['price'].astype('float64')
        # clean 'mileage' column 
        car_df['mileage'] = car_df['mileage'].str.replace(',', '')
        car_df['mileage'] = car_df['mileage'].astype('float64')
        # clean 'doors' column 
        car_df['doors'] = car_df['doors'].replace(0, np.nan)
        car_df['doors'] = car_df['doors'].astype('int32')
        # clean 'engine_size' column 
        df_mask = car_df[['engine_size']]
        mask = car_df['engine_size'].str.contains(r'g', na=False)
        df_mask.loc[mask, 'engine_size'] = np.nan
        car_df['engine_size'] = df_mask['engine_size']
        car_df['engine_size'] = car_df['engine_size'].str.replace(' L', '')
        car_df['engine_size'] = car_df['engine_size'].astype('float64')
        # save cleaned data
        car_df.to_csv('Output Test.csv', encoding='utf8')
        

    def __del__(self):
        # shut down the browser
        self.driver.close()  
        print('>>>>[Well Done]')

# test code
if __name__ == '__main__':
    test = Car()
    test.run()
    test.create_df()
    test.clean_df()