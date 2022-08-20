import urllib
from copy import deepcopy
from itertools import permutations
from urllib.parse import urlencode

import scrapy
from scrapy import Request


class RealtorsSpider(scrapy.Spider):
    name = 'Realtors'
    base_url = 'https://ims.maarnet.org'
    search_page_url = 'https://ims.maarnet.org/scripts/mgrqispi.dll?APPNAME=IMS&PRGNAME=IMSMemberLogin&ARGUMENTS' \
                      '=-AMAAR&SessionType=N&ServiceName=OSRH&NotLogin=Y '

    custom_settings = {'ROBOTSTXT_OBEY': False, 'LOG_LEVEL': 'INFO',
                       'CONCURRENT_REQUESTS_PER_DOMAIN': 5,
                       'RETRY_TIMES': 5,
                       # 'FEED_EXPORT_FIELDS': [ 'Business Name', 'Full Name', 'First Name', 'Last Name',
                       # 'Street Address', 'State', 'Zip', 'Description', 'Phone Number', 'Email', 'Business_Site',
                       # 'Social_Media', 'Category', 'Rating', 'Reviews', 'Source_URL', 'Detail_Url', 'Services',
                       # 'Latitude', 'Longitude', 'Occupation', 'Phone_Type', 'Lead_Source', 'State_Abrv',
                       # 'State_TZ', 'State_Type', 'SIC_Sectors', 'SIC_Categories', 'SIC_Industries', 'NAICS_Code',
                       # 'Quick_Occupation', ],
                       'FEED_URI': 'output.csv',
                       'FEED_FORMAT': 'csv',
                       }

    def start_requests(self):
        yield Request(self.search_page_url, self.parse)

    def parse(self, response):
        argument_token = urllib.parse.quote(response.xpath("//input[@name='ARGUMENTS']/@value").get())
        api_url = 'https://ims.maarnet.org/scripts/mgrqispi.dll?APPNAME=IMS&PRGNAME=IMSAgentsandOffices&ARGUMENTS={' \
                  '}&Search_Type=A&LastName=&%28P%29+Agent+Nickname={' \
                  '}&%28P%29+Office+Name=&%28P%29+City=&%28P%29+Zip+Code= '
        keywords = self.get_search_keywords()
        for char in keywords:
            relative_url = deepcopy(api_url.format(argument_token, char))
            yield Request(relative_url, callback=self.parse_listing)

    def parse_listing(self, response):
        members_list = response.xpath("//table[@id='rapSearchMemberResults']/tr")
        for member in members_list:
            member_name = member.xpath("./td[1]/a/text()").get()
            detail_url = member.xpath("./td[1]/a/@href").get()
            if not detail_url.startswith(self.base_url):
                detail_url = self.base_url + detail_url
            yield Request(detail_url, callback=self.parser_details, meta={'member_name': member_name})

    def parser_details(self, response):
        yield {
            'Business Name': response.xpath(
                "//table[@class='table table-striped']/tr[contains(td/strong/text(),'Office:')]/td[2]/a/text()").get().strip(),
            'Full Name': response.meta['member_name'],
            'First Name': response.meta['member_name'].split(',')[0],
            'Last Name': response.meta['member_name'].split(',')[1],
            'Street Address': response.xpath(
                "//table[@class='table table-striped']/tr[contains(td/strong/text(),'Office:')]/td[2]/text()[2]").get().strip(),
            'State': response.xpath(
                "//table[@class='table table-striped']/tr[contains(td/strong/text(),'Office:')]/td[2]/text()[3]").get().strip(),
            'Zip': response.xpath(
                "//table[@class='table table-striped']/tr[contains(td/strong/text(),'Office:')]/td[2]/text()[4]").get().strip(),
            'Description': '',
            'Phone Number': response.xpath(
                "//table[@class='table table-striped']/tr[contains(td/strong/text(),'Phone')]/td[2]/text()").get(),
            'Fax': response.xpath(
                "//table[@class='table table-striped']/tr[contains(td/strong/text(),'Fax')]/td[2]/text()").get(),
            'Email': response.xpath(
                "//table[@class='table table-striped']/tr[contains(td/strong/text(),'E-Mail:')]/td[2]/a/text()").get(),
            'Business_Site': '',
            'Social_Media': '',
            'Category': '',
            'Rating': '',
            'Reviews': '',
            'Source_URL': '',
            'Detail_Url': response.url,
            'Services': '',
            'Latitude': '',
            'Longitude': '',
            'Occupation': '',
            'Phone_Type': '',
            'Lead_Source': '',
            'State_Abrv': '',
            'State_TZ': '',
            'State_Type': '',
            'SIC_Sectors': '',
            'SIC_Categories': '',
            'SIC_Industries': '',
            'NAICS_Code': '',
            'Quick_Occupation': '',

        }

    def get_search_keywords(self):
        alphabats = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
                     'u', 'v', 'w', 'x', 'y', 'z']
        r = 2
        alphabats_per = [''.join(p) for p in permutations(alphabats, r)]
        return alphabats + alphabats_per
