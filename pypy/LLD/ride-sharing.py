from datetime import datetime, timedelta
from abc import ABC, abstractmethod


class User:
    def __init__(self, user_id, name):
        self.user_id = user_id
        self.name = name
        self.vehicles = []  # Vehicles owned by the user
        self.rides_offered = 0
        self.rides_taken = 0

class Vehicle:
    def __init__(self, vehicle_id, owner, make, model, registration_number):
        self.vehicle_id = vehicle_id
        self.owner = owner
        self.make = make
        self.model = model
        self.registration_number = registration_number

class Ride:
    def __init__(self, ride_id, driver, vehicle, origin, destination, start_time, ride_time, available_seats):
        self.ride_id = ride_id
        self.driver = driver
        self.vehicle = vehicle
        self.origin = origin
        self.destination = destination
        self.start_time = start_time
        self.ride_time = ride_time
        self.available_seats = available_seats
        self.end_time = self.calculate_end_time()

    def calculate_end_time(self):
        return self.start_time + timedelta(hours=self.ride_time)

class RideSelectionStrategy(ABC):
    @abstractmethod
    def choose_strategy(self, rides):
        ...

class FastestRideStrategy(RideSelectionStrategy):
    def choose_strategy(self, rides):
        return min(rides, key=lambda x: x.ride_time)

class EarliestRideStrategy(RideSelectionStrategy):
    def choose_strategy(self, rides):
        return min(rides, key=lambda x: x.end_time)

class RideShareService:
    def __init__(self):
        self.users = {}
        self.vehicles = {}
        self.rides = []

    def register_user(self, user_id, name):
        if user_id not in self.users:
            self.users[user_id] = User(user_id, name)
        return self.users[user_id]

    def add_vehicle(self, user_id, vehicle_id, make, model, registration_number):
        if user_id in self.users and vehicle_id not in self.vehicles:
            vehicle = Vehicle(vehicle_id, self.users[user_id], make, model, registration_number)
            self.vehicles[vehicle_id] = vehicle
            self.users[user_id].vehicles.append(vehicle)
        return vehicle

    def offer_ride(self, user_id, vehicle_id, origin, destination, start_time, ride_time, available_seats):
        if user_id in self.users and vehicle_id in self.vehicles:
            ride = Ride(len(self.rides) + 1, self.users[user_id], self.vehicles[vehicle_id], origin, destination, start_time, ride_time, available_seats)
            self.rides.append(ride)
            self.users[user_id].rides_offered += 1
            return ride
        return None

    def search_rides(self, origin, destination):
        return [ride for ride in self.rides if ride.origin == origin and ride.destination == destination and ride.available_seats > 0]

    def select_ride(self, user_id, origin, destination, selection_strategy):
        available_rides = self.search_rides(origin, destination)
        if not available_rides:
            return None
        selected_ride = selection_strategy.choose_strategy(available_rides)
        if not selected_ride:
            return None
        selected_ride.available_seats -= 1
        self.users[user_id].rides_taken += 1
        return selected_ride

    def get_user_stats(self, user_id):
        if user_id in self.users:
            user = self.users[user_id]
            return (user.rides_offered, user.rides_taken)
        return (0, 0)
    
    def get_fuel_details(self):
        pass


# Code Runner with tests
class DemoDriver:
    @staticmethod
    def run_demo():
        service = RideShareService()
        # Register users
        alice = service.register_user("1", "Alice")
        bob = service.register_user("2", "Bob")
        Ammy = service.register_user("3", "Ammy")

        # Alice adds her vehicle
        service.add_vehicle("1", "V1", "Toyota", "Fortuner", "KA01JU0001")
        
        # Bob adds his vehicle
        service.add_vehicle("2", "V2", "BMW", "M1", "KA01JU0002")

        # Alice offers a ride from Point A to Point B
        service.offer_ride("1", "V1", "Point A", "Point B", datetime.now(), 2, 3)
        
        # Bob offers another ride from Point A to Point B, but faster
        service.offer_ride("2", "V2", "Point A", "Point B", datetime.now() + timedelta(hours=1), 1.5, 2)

        # Ammy looks for a ride from Point A to Point B
        available_rides = service.search_rides("Point A", "Point B")
        print("Available rides from Point A to Point B:")
        for ride in available_rides:
            print(f"Ride ID: {ride.ride_id}, Driver: {ride.driver.name}, Vehicle: {ride.vehicle.make} {ride.vehicle.model}, Ride Time: {ride.ride_time}hrs")

        # Ammy selects the fastest ride
        selected_ride = service.select_ride("3", "Point A", "Point B", FastestRideStrategy())
        print(f"\nAmmy selected the fastest ride: Ride ID {selected_ride.ride_id}, Driver: {selected_ride.driver.name}, Ride Time: {selected_ride.ride_time}hrs")

        # User stats
        alice_stats = service.get_user_stats("1")
        bob_stats = service.get_user_stats("2")
        Ammy_stats = service.get_user_stats("3")
        print(f"\nUser Stats:\nAlice - Rides Offered: {alice_stats[0]}, Rides Taken: {alice_stats[1]}\nBob - Rides Offered: {bob_stats[0]}, Rides Taken: {bob_stats[1]}\nAmmy - Rides Offered: {Ammy_stats[0]}, Rides Taken: {Ammy_stats[1]}")

# Running the demo
DemoDriver.run_demo()