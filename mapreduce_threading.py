import concurrent.futures  #Use this library to implement multithreaded concurrent execution tasks
from collections import defaultdict

#The implementation of the following basic functions is not very different from single threading,
# the main improvement is the addition of multithreading.
def mapper(line):
    passenger_id, _, _, _, _, _ = line.strip().split(',')
    return passenger_id, 1

def reducer(key, values):
    total_flights = sum(values)
    return key, total_flights

# Processing passenger data, mapping each row of data to a list of key-value pairs by mapper function
def process_passenger_data(passenger_data):
    intermediate_data = []
    for line in passenger_data:
        intermediate_data.append(mapper(line))
    return intermediate_data

# Process a line of airport data, breaking it into tuples of airport codes and airport information
def process_airport_data(airport_data_line):
    airport_name, airport_code, latitude, longitude = airport_data_line.strip().split(',')
    return airport_code, (airport_name, float(latitude), float(longitude))

# Count the number of flights at each airport
def process_passenger_flights(passenger_data):
    airport_flights = defaultdict(int)
    for line in passenger_data:
        _, from_airport, to_airport, _, _, _ = line.strip().split(',')
        airport_flights[from_airport] += 1
        airport_flights[to_airport] += 1
    return airport_flights

# Read passenger data
passenger_data = []
with open('AComp_Passenger_data_no_error.csv', 'r') as file:
    passenger_data = file.readlines()

# Read airport data, read Airport Data with Concurrency
airport_data = {}
with open('Top30_airports_LatLong.csv', 'r') as file:
    airport_data_lines = file.readlines()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        processed_airport_data = executor.map(process_airport_data, airport_data_lines)
    airport_data = dict(processed_airport_data)

# Mapper with Concurrency
with concurrent.futures.ThreadPoolExecutor() as executor:
    intermediate_data = executor.submit(process_passenger_data, passenger_data).result()

# Group by passenger ID
grouped_data = defaultdict(list)
for passenger_id, value in intermediate_data:
    grouped_data[passenger_id].append(value)

# Reducer,the reducer function is executed concurrently using ThreadPoolExecutor and executor.map.
with concurrent.futures.ThreadPoolExecutor() as executor:
    result = list(executor.map(reducer, grouped_data.keys(), grouped_data.values()))

# Find passenger with the highest number of flights
max_passenger = max(result, key=lambda x: x[1])

# Output the result
print(f"Passenger with the highest number of flights: {max_passenger[0]}")
print(f"Number of flights: {max_passenger[1]}")
print("==================================================")
print("==================================================")


