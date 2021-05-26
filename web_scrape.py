import requests
import psycopg2 as pg2
from bs4 import BeautifulSoup

def check_exists(parcel_str, cursor):
    cursor.execute("SELECT PARCEL_STR FROM property_data WHERE PARCEL_STR = %s", (parcel_str,))
    return cursor.fetchone() is None

def update_database(address, summ_dict, desc_dict, connection, cursor):
    
    if check_exists(summ_dict['parcel_str'], cursor):
        insert_query = """
            INSERT INTO property_data (
                PARCEL_STR
                ,ADDRESS
                ,OWNER_NAME
                ,OWNER_ADDRESS_1
                ,OWNER_ADDRESS_2
                ,PROPERTY_DESC
                ,PROPERTY_TYPE
                ,TAX_DISTRICT
                ,STYLE
                ,SQFT
                ,BEDROOMS
                ,BATHS_FULL_HALF
                ,YEAR_BUILT
                ,BASEMENT_FINISH
                ,LOT_SIZE
                ,ZONING
                ,MILL_LEVY
                ,DOC_TYPE
            ) VALUES (%s,%s,%s,%s,%s
                    ,%s,%s,%s,%s,%s
                    ,%s,%s,%s,%s,%s
                    ,%s,%s,%s)
        """

        record_values = (summ_dict['parcel_str']
                        ,address
                        ,summ_dict['owner']['name']
                        ,summ_dict['owner']['address_1']
                        ,summ_dict['owner']['address_2']
                        ,summ_dict['description']
                        ,summ_dict['property_type']
                        ,summ_dict['tax_district']
                        ,desc_dict['Style']
                        ,desc_dict['Building Sqr. Foot']
                        ,desc_dict['Bedrooms']
                        ,desc_dict['Baths Full/Half']
                        ,desc_dict['Effective Year Built']
                        ,desc_dict['Basement/Finish']
                        ,desc_dict['Lot Size']
                        ,desc_dict['Zoned As']
                        ,desc_dict['Mill Levy']
                        ,desc_dict['Document Type'])

        cursor.execute(insert_query, record_values)

        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into Denver Property table")
    else:
        print(f"Record already exists for {summ_dict['parcel_str']}") 

if __name__ == '__main__':

    connection = pg2.connect(database="denver_properties", 
                            user='postgres', 
                            password='2855Wakonda',         
                            host='127.0.0.1', 
                            port= '5432')

    cursor = connection.cursor()

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'}
            
    url_str = 'https://www.denvergov.org/property/realproperty/summary/'
    assessment_map_number = [str(i).zfill(5) for i in range(1320, 1350)]
    assessor_block_number = [str(i).zfill(2) for i in range(1,100)]
    property_id = [str(i).zfill(3) for i in range(1,100)]

    for map in assessment_map_number:
        no_parcel_count = 0 
        for block in assessor_block_number:
            for id in property_id:
                parcel = map + block + id + '000'
                r = requests.get(url_str+parcel, timeout=600, headers=headers)
                soup = BeautifulSoup(r.text, 'html.parser')
                if soup.find('h1').string.strip() == 'Oops! Something went wrong!':
                    print(f"No more valid parcels for {map}-{block}")
                    no_parcel_count += 1
                    break
                else:
                    address = soup.find(lambda tag: (tag.name == 'h1' and tag.get('id') == 'address')) 

                    table_search = soup.find_all('table')
                    property_summary = table_search[0]
                    table_info = property_summary.find_all('td')
                    owner_info = table_info[0].find_all('div')
                    owner_dict = {'name': owner_info[0].string.strip()
                                ,'address_1': owner_info[2].string.strip().replace('   ', ' ')
                                ,'address_2': owner_info[3].string.strip()
                                }

                    summ_dict = {'owner': owner_dict
                                ,'parcel_str': parcel[0:5]+'-'+parcel[5:7]+'-'+parcel[7:]
                                ,'description': table_info[3].string.strip()
                                ,'property_type': table_info[4].string.strip()
                                ,'tax_district': table_info[5].string.strip()}

                    property_description = soup.find(lambda tag: (tag.name == 'div' and tag.get('id') == 'property_summary'))
                    rows = property_description.find_all('td')
                    desc_dict = {}
                    for i in range(0,20,2):
                        if i in [2,4,8,12]:
                            try:
                                desc_dict[rows[i].string.replace(': ','')] = int(rows[i+1].string.replace(',',''))
                            except:
                                desc_dict[rows[i].string.replace(': ','')] = rows[i+1].string
                        elif i == 16:
                            desc_dict[rows[i].string.replace(': ','')] = float(rows[i+1].string)
                        else:
                            desc_dict[rows[i].string.replace(': ','')] = rows[i+1].string
                    
                    update_database(address.string, summ_dict, desc_dict, connection, cursor)

                    no_parcel_count = 0
                    
            if no_parcel_count >= 10:
                print(f"Map {map} is probably not a valid map number")
                break