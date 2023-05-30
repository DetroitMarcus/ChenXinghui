from collections import defaultdict

def mapper(line):
    #It yields a key-value pair where the key is the passenger ID and the value is a constant 1.
    passenger_id, _, _, _, _, _ = line.strip().split(',')
    yield passenger_id, 1

def reducer(key, values):
    #It calculates the total number of flights by summing the values and yields the key-value pair
    total_flights = sum(values)
    yield key, total_flights

# Read passenger data
passenger_data = []
with open('AComp_Passenger_data_no_error.csv', 'r') as file:
    passenger_data = file.readlines()

# Read airport data
airport_data = {}
with open('Top30_airports_LatLong.csv', 'r') as file:
    for line in file:
        airport_name, airport_code, latitude, longitude = line.strip().split(',')
        airport_data[airport_code] = (airport_name, float(latitude), float(longitude))

# Mapper，The resulting key-value pairs are stored in the intermediate_data list.
intermediate_data = []
for line in passenger_data:
    intermediate_data.extend(mapper(line))

# Group by passenger ID，this code block groups the key-value pairs based on the passenger ID
grouped_data = defaultdict(list)
for passenger_id, value in intermediate_data:
    grouped_data[passenger_id].append(value)

# Reducer,the reducer function is applied to each passenger ID and their corresponding list of flights.
result = []
for passenger_id, values in grouped_data.items():
    result.extend(reducer(passenger_id, values))

# Find passenger with highest number of flights
max_passenger = max(result, key=lambda x: x[1])

# Output the result
print(f"Passenger with the highest number of flights: {max_passenger[0]}")
print(f"Number of flights: {max_passenger[1]}")
print(f"==================================================")
print(f"==================================================")


# Get airport details for the passenger's flights
passenger_flights = max_passenger[1]
passenger_id = max_passenger[0]

airport_flights = defaultdict(int)
for line in passenger_data:
    _, from_airport, to_airport, _, _, _ = line.strip().split(',')
    airport_flights[from_airport] += 1
    airport_flights[to_airport] += 1

# print("Airports visited by the passenger:")
# for airport_code, count in airport_flights.items():
#     if count > 0:
#         airport_name, latitude, longitude = airport_data.get(airport_code, ("Unknown", None, None))
#         print(f"IATA/FAA Code: {airport_code}")
#         print(f"Airport Name: {airport_name}")
#         print(f"Latitude: {latitude}")
#         print(f"Longitude: {longitude}")
#         print(f"Number of Flights: {count}")
#         print()
