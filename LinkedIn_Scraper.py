from selenium import webdriver
#import seleniumwire.undetected_chromedriver as uc
import undetected_chromedriver.v2 as uc
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
import pandas as pd
import numpy as np
from pathlib import Path
import time
import os
import random
import shutil
import time
import unidecode
from multiprocessing import freeze_support
from datetime import datetime
import pycountry
import sys

def get_linkedin_data(brands, positions, locations, exclude, npages):

    start_time = time.time()
    # configuring the path for the output file
    path = os.getcwd()
    stamp = datetime.now().strftime("%d_%m_%Y_%I_%M_%p")
    if '\\' in path:
        path += f'\\Brands_Contacts_{stamp}.xlsx'
    else:
        path += f'/Brands_Contacts_{stamp}.xlsx'

    df = pd.DataFrame()

    nbrands = len(brands)
    print('-'*75)
    print('Initializing the web bot ...')
    print('-'*75)
    driver = initialize_bot()

    print('Searching the brands contacts ...')
    print('-'*75)

    # for non specified positions or locations
    if not locations:
        locations.append('')
    if not positions:
        positions.append('')

    npos = len(positions)
    nloc = len(locations)

    # getting country abbreviations
    countries = {}
    for country in pycountry.countries:
        countries[country.name] = country.alpha_2

    exclude_abb = []
    for elem in exclude:
        if elem == '': continue
        exclude_abb.append(countries[elem.title()].lower())

    # searching google for the contacts
    for i, brand in enumerate(brands):
        if brand == '': continue
        for pos in positions:
            if pos == '' and npos > 1: continue
            for loc in locations:
                if loc == '' and nloc > 1: continue
                link = ''
                data = []
                # skip invalid brand names
                if len(brand) == 0:
                    continue
                # get the results from the first 3 pages
                for j in range(npages):
                    try:
                        # iterating the user agent for the bot
                        agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{}.0.0.0 Safari/537.36'.format(random.randint(90, 105))
                        driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": agent})

                        if link == '':
                            if len(loc) > 0:
                                link = 'https://www.google.com/search?q=' + f"{pos} LinkedIn profiles" + ' ' + f'for people working in {brand}  {loc}'+ '&start=00'
                            else:
                                link = 'https://www.google.com/search?q=' + f"{pos} LinkedIn profiles" + ' ' + f'for people working in {brand}'+ '&start=00'

                            driver.get(link)
                            # displaying results in English
                            buttons = wait(driver, 5).until(EC.presence_of_all_elements_located((By.TAG_NAME, "a")))
                            for button in buttons:
                                if 'Change to English' in button.text:
                                    driver.execute_script("arguments[0].click();", button)
                                    time.sleep(2)
                                    break

                        results = wait(driver, 5).until(EC.presence_of_all_elements_located((By.XPATH, "//div[@id='rso']/div")))
                        for res in results:
                            try:
                                a = wait(res, 5).until(EC.presence_of_element_located((By.TAG_NAME, "a")))
                                url = a.get_attribute('href')
                                if 'linkedin' not in url: continue
                                skip = False
                                for abb in exclude_abb:
                                    if abb + '.' in url: 
                                        skip = True
                                if skip: continue
                                des = wait(a, 5).until(EC.presence_of_element_located((By.TAG_NAME, "h3"))).text
                                name = des.split('-')[0]
                                # relative xpath
                                details = wait(res, 5).until(EC.presence_of_element_located((By.XPATH, ".//div[@class='MUxGbd wuQ4Ob WZ8Tjf']"))).text
                                # check for valid details for processing
                                if '·' not in details: continue
                                info = details.split('·')
                                if len(info) < 3: continue
                                # check the location
                                if len(loc) > 0:
                                    if loc.lower().strip() not in info[0].lower() and loc.lower().strip() not in info[1].lower(): continue
                                # skipping counties in exclude list
                                if info[0].lower().strip() in exclude or info[1].lower().strip() in exclude:
                                    continue
                                # check the position
                                if len(pos) > 0:
                                    if pos.lower().strip() not in info[1].lower(): continue
                                # check the brand
                                if brand.lower().strip() not in info[2].lower(): continue
                                curr_pos = info[1]
                                data.append({'Brand':unidecode.unidecode(brand).strip(), "Searched Position":pos.title().strip(), "Searched Location":loc.title().strip(),"Contact Name":unidecode.unidecode(name).strip(), 'Position':unidecode.unidecode(curr_pos).strip(), 'LinkedIn Link':url})
                            except:
                                pass
                        if not data:
                            data.append({'Brand':unidecode.unidecode(brand).strip(), "Searched Position":pos.title().strip(), "Searched Location":loc.title().strip(),"Contact Name":'No Results', 'Position':'No Results', 'LinkedIn Link':'No Results'})

                        df = df.append(data)
                        # output scraped data to csv each 100 keywords
                        if np.mod(i+1, 50) == 0:
                            print('Outputting scraped data to csv file ...')
                            df.to_excel(path, index=False)
                            driver.quit()
                            time.sleep(2)
                            driver = initialize_bot()
                        time.sleep(2)

                        link = link[:-2] + f'{j+1}' + '0'
                        driver.get(link)
                        time.sleep(1)
   
                    except Exception as err:
                        # handling errors
                        print('-'*75)
                        print('The below error occurred, restarting :')
                        print(str(err))   
                        print('-'*75)
                        print('Restarting the session ....')
                        print('-'*75)
                        df.to_excel(path, index=False)
                        driver.quit()
                        time.sleep(2)
                        driver = initialize_bot()
                        continue

        # restarting the bot each brand
        print(f'contacts of brand {i+1}/{nbrands} are scraped successfully.')     
        driver.quit()
        driver = initialize_bot()

    print('Processing the output data ...')
    driver.quit()
    # remove duplicate records
    df.drop_duplicates(inplace=True)
    # output the dataframe to a csv 
    df.to_excel(path, index=False)
    mins = round((time.time() - start_time)/60, 2)
    hrs = round(mins / 60, 2)
    print('-'*75)
    print(f'process completed successfully! Elsapsed time {hrs} hours ({mins} mins)')
    print('-'*75)
    input('Press any key to exit.')

def initialize_bot():

    class Spoofer(object):

        def __init__(self):
            self.userAgent = self.get()

        def get(self):
            ua = ('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{}.0.0.0 Safari/537.36'.format(random.randint(90, 140)))

            return ua

    class DriverOptions(object):

        def __init__(self):

            self.options = uc.ChromeOptions()
            self.options.add_argument('--log-level=3')
            self.options.add_argument('--start-maximized')
            self.options.add_argument('--disable-dev-shm-usage')
            self.options.add_argument("--incognito")
            self.options.add_argument('--disable-popup-blocking')
            self.options.add_argument("--headless")
            self.helperSpoofer = Spoofer()
            #self.seleniumwire_options = {}
           
            # random user agent
            self.options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{}.0.0.0 Safari/537.36'.format(random.randint(90, 140)))
            self.options.page_load_strategy = 'eager'
           
            # Create empty profile for non Windows OS
            if os.name != 'nt':
                if os.path.isdir('./chrome_profile'):
                    shutil.rmtree('./chrome_profile')
                os.mkdir('./chrome_profile')
                Path('./chrome_profile/First Run').touch()
                self.options.add_argument('--user-data-dir=./chrome_profile/')
   
            # using proxies without credentials
            #if proxies:
            #   self.options.add_argument('--proxy-server=%s' % self.helperSpoofer.ip)


    class WebDriver(DriverOptions):

        def __init__(self):
            DriverOptions.__init__(self)
            self.driver_instance = self.get_driver()

        def get_driver(self):

            webdriver.DesiredCapabilities.CHROME['acceptSslCerts'] = True
      
            # uc Chrome driver
            #driver = uc.Chrome(options=self.options, seleniumwire_options=self.seleniumwire_options)
            driver = uc.Chrome(options=self.options)
            driver.set_page_load_timeout(30)
            driver.command_executor.set_timeout(30)

            return driver

    driver= WebDriver()
    driverinstance = driver.driver_instance
    return driverinstance

def get_inputs():

    ## number of results pages to scrape
    while True: 
        print('-'*75)
        npages = input('Please Enter The Number Of The Results Pages To Be Scraped: ')
        try:
            npages = int(npages)
        except:
            print('Invalid Input. The Supported Values Are 1-3, Please Try Again! ')
            continue
        if npages > 0 and npages < 4:
            break
        else:
            print('Invalid Input. The Supported Values Are 1-3, Please Try Again! ')

  
    ## assuming brands sheet to be the same directory of the script
    path = os.getcwd()
    if '//' in path:
        path += '//Inputs.xlsx'
    else:
        path += '\Inputs.xlsx'

    if not os.path.isfile(path):
        print('Error: Missing the input sheet "Inputs.xlsx"')
        input('Press any key to exit')
        sys.exit(1)

    print('-'*75)
    print('Processing the input data...')
    try:
        df = pd.read_excel(path)      
    except:
        print('Error: Failed to process the input sheet')
        input('Press any key to exit')
        sys.exit(1)

    df[['Brand', 'Position', 'Location', 'Exclude Location']] = df[['Brand', 'Position', 'Location', 'Exclude Location']].astype(str)
    df['Brand'] = df['Brand'].apply(lambda x: x.title())
    df['Brand'] = df['Brand'].str.title().str.strip().replace('Nan', '')
    brands = df['Brand'].unique().tolist()
    if '' in brands:
        brands.remove('')
    df['Position'] = df['Position'].str.title().str.strip().replace('Nan', '')
    positions = df['Position'].unique().tolist()
    df['Location'] = df['Location'].str.title().str.strip().replace('Nan', '')
    locations = df['Location'].unique().tolist()
    df['Exclude Location'] = df['Exclude Location'].str.lower().str.strip().replace('nan', '')
    exclude = df['Exclude Location'].unique().tolist()

    return brands, positions, locations, exclude, npages
    



if __name__ == "__main__":

    freeze_support()
    # reading the brands from a given csv file
    brands, positions, locations, exclude, npages = get_inputs()
    # searching for the linkedin pages 
    try:
        get_linkedin_data(brands, positions, locations, exclude, npages)
    except Exception as err:
        print('The below error occurred:')
        err = str(err)
        if 'Stacktrace' in err:
            print(err[:err.index('Stacktrace')])
        else:
            print(err)
    input('Press any key to exit')