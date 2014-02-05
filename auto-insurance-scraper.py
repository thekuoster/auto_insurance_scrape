#!/usr/local/bin/python

"""Scraper to get Auto Insurance information

Comment out compileURLs in main if you don't want to
run this each time.

Usage: ./auto-insurance-scarper.py
"""

import csv
import lxml.html
import multiprocessing
import os
import re
import traceback
import urllib2

from lxml.html import parse


PREFIX = 'http://www.valuepenguin.com'
AUTO_INSURANCE = PREFIX + '/auto-insurance'

URL_OUTPUT = './city_urls.csv'
DATA_OUTPUT = './auto_insurance_info.csv'
COMPLETED = './completed.csv'
ERRORS = './errors.csv'

def compileURLs():
  state_urls = []
  city_urls = []

  # read in website
  main_site = lxml.html.parse(AUTO_INSURANCE)

  # grab all state urls
  states_div = main_site.xpath('//div[@id="directoryAutoInsuranceStatesSmall"]')[0]
  states = states_div.findall('.//a')
  
  state_urls.extend([link.get('href') for link in states])

  # compile city urls into csv
  with open(URL_OUTPUT, 'wb') as city_file:
    city_writer = csv.writer(city_file)
    for state_url in state_urls:
      state_site = lxml.html.parse(PREFIX + state_url)
      cities_div = state_site.xpath('//div[@class="directoryListingContainer"]')[0]
      cities = cities_div.findall('.//a') 

      # write the city urls to a csv file
      for city in cities:
        city_writer.writerow([PREFIX + city.get('href')])

def getInsuranceData():
  """Runs multiprocessor to get insurance data for all cities"""
  city_urls = []
  completed = []

  # read in all city urls
  with open(URL_OUTPUT) as city_file:
    city_reader = csv.reader(city_file)
    for row in city_reader:
      city_urls.append(row[0])

  # reads in urls that have been parsed already
  try:
    with open(COMPLETED) as completed_file:
      complete_reader = csv.reader(completed_file)
      for row in complete_reader:
        completed.append(row[0])
  except:
    pass

  # creates a new output if no urls have been completed
  if not completed:
    with open(DATA_OUTPUT, 'wb') as data_file:
      data_writer = csv.writer(data_file)
      data_writer.writerow(['State', 'City', 'Insurance Company Name',
          'Address', 'Phone Number', 'Fax', 'Email'])

  # amend url list to only include ones that haven't been parsed
  for url in completed:
    city_urls.remove(url)

  # Use multiple threads to speed up scraping
  pool = multiprocessing.Pool()

  # Do parsing in chunks so we don't need to start at the beginning
  # if it breaks 
  chunks = [city_urls[i:i+100] for i in xrange(0, len(city_urls), 100)]
  for chunk in chunks:
    results = pool.map(extractCityData, [city_url for city_url in chunk])

    # add completed urls to csv
    with open(COMPLETED, 'a+b') as completed_file:
      complete_writer = csv.writer(completed_file)
      for url in chunk:
        complete_writer.writerow([url])

    # add insurance data to csv file
    with open(DATA_OUTPUT, 'a+b') as data_file:
      data_writer = csv.writer(data_file)

      for result in results:
        if result is not None:
          for info in result:
            data_writer.writerow(info)

def extractCityData(city_url):
  """Extracts insurance data from a single city page"""
  city_data = []

  error_file = csv.writer(open(ERRORS, 'wb'))

  # get state and city name
  city = os.path.basename(city_url).replace('-', ' ')
  state = os.path.basename(os.path.dirname(city_url)).replace('-', ' ')

  # grab html of city page
  try:
    city_site = lxml.html.parse(city_url)
  except:
    error_file.writerow([city_url])
    print city_url
    print traceback.format_exc()
    return None

  # record info for all companies
  companies = city_site.xpath('//div[@class="directoryAgent"]')
  for company in companies:
    company_name = company.find(
        './/div[@class="directoryAgentName"]').text_content()

    # pull out company address
    company_address = ''
    company_address_raw = company.find(
        './/div[@class="directoryAgentAddress"]').find('./p').text_content()
    # reformat address, get rid of line breaks
    company_address = normalizeWhitespace(company_address_raw)

    # pull out other contact info
    contact_info_raw = company.find(
      './/div[@class="directoryAgentInfo"]').find('./p').text_content()
    contact_info = normalizeWhitespace(contact_info_raw)
    (phone, fax, email) = extractInfo(contact_info)

    info = [state, city, company_name,
        company_address, phone, fax, email]
    print info

    city_data.append(info)

  return city_data

def extractInfo(contact_info):
  """Parse out Phone, Fax, and Email info"""

  fax = ''
  phone = ''
  email = ''

  info_parts = re.split('(?<!:) ', contact_info)
  
  for part in info_parts:
    # extract data
    info = part.split()[-1]
    # determine type of data
    if 'Phone' in part:
      phone = info
    elif 'Fax' in part:
      fax = info
    elif 'Email' in part:
      email = info

  return (phone, fax, email)

def normalizeWhitespace(text_raw):
  return re.subn(r'\s+', ' ', text_raw)[0].strip()

def main():
  compileURLs()
  getInsuranceData()

if __name__ == '__main__':
  main()
