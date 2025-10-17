import random
from faker import Faker
from datetime import timedelta
import geonamescache
import csv

# Set ups
fake = Faker()  # Initialize Faker for generating fake data
gc = geonamescache.GeonamesCache()  # Initialize geonamescache - used to get city and country data

nums_rows = 15000  # Number of rows to generate
rogue_count = 150 # Number of rogue rows to generate

#--------------------------------------------------------------
# Real City-Country data pairs
allowed_countries = [
    "United States", "Canada", "United Kingdom", "Germany", "France",
    "Italy", "Spain", "Australia", "Japan", "Brazil", "India", "Mexico"
]

# Get random city and country
def get_random_city_country():
    # Select a random city from the list of cities
    city = random.choice(filtered_cities)
    # Get the country code for the selected city
    country_code = city["countrycode"]
    # Get the country name using the country code
    country = countries[country_code]["name"]
    # Return the city name and country name
    return city["name"], country

# Get the list of cities and countries from geonamescache
cities = list(gc.get_cities().values())
countries = gc.get_countries()

# Pick only cities that belong to allowed countries
allowed_country_codes = [code for code, info in countries.items() if info['name'] in allowed_countries]     # Get country codes for allowed countries
filtered_cities = [city for city in cities if city['countrycode'] in allowed_country_codes] # Filter cities based on allowed country codes

#--------------------------------------------------------------
# Create valid records
def create_valid_record():
    city, country = get_random_city_country()
    date_time = fake.date_time_between(start_date='-10y', end_date='now')  # Random date within the last 10 years
    return [date_time.strftime('%Y-%m-%d %H:%M:%S'), country, city]

# Create rogue records
def create_rogue_record():
    rogue_types = ['bad_date', 'wrong_country', 'wrong_city']
    rtype = random.choice(rogue_types)
    city, country = get_random_city_country()

    if rtype == 'bad_date':
        # Date that is in the future
        date_time = fake.date_time_between(start_date='-10y', end_date='now') + timedelta(days=365*30)
        price = round(random.uniform(1.0, 3500.0), 2)
        return [price, date_time.strftime('%Y-%m-%d %H:%M:%S'), country, city]
    
    elif rtype == 'wrong_country':
        # Country that doesn’t match the city (or even exist in dataset)
        wrong_country = fake.country()
        price = round(random.uniform(1.0, 3500.0), 2)
        date_time = fake.date_time_between(start_date='-10y', end_date='now')
        return [price, date_time.strftime('%Y-%m-%d %H:%M:%S'), wrong_country, city]
    
    elif rtype == 'wrong_city':
        # City that isn’t real or doesn’t match the country
        wrong_city = fake.city()
        price = round(random.uniform(1.0, 3500.0), 2)
        date_time = fake.date_time_between(start_date='-10y', end_date='now')
        return [price, date_time.strftime('%Y-%m-%d %H:%M:%S'), country, wrong_city]

#--------------------------------------------------------------
# Main function to generate data
def main():
    # Generate Data
    data = []

    # Add normal records
    for i in range(nums_rows - rogue_count):
        record = create_valid_record()
        data.append(record)

    # Add rogue records
    for i in range(rogue_count):
        record = create_rogue_record()
        data.append(record)
    
    # Shuffle the data to mix rogue records with valid records
    random.shuffle(data)

    # Print sample data
    for row in data[:10]:  # only print the first 10
        print(row)
    print(f"\nTotal records: {len(data)} (including {rogue_count} rogue records)\n")

    # Create CSV file
    with open('E_Commerce_data_Victoria.csv', mode='w', newline='', encoding = 'utf-8') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['Date', 'Country', 'City'])
        # Write data rows
        writer.writerows(data)
    print(f"Generated {nums_rows} rows of e-commerce data with {rogue_count} rogue entries.")


if __name__ == "__main__":
    main()