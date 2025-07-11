#!/usr/bin/env python3
"""
Sample Data Generator for Shipment Timeline Workflow Demo

Generates realistic sample shipment events for testing the incremental processing pipeline.
Creates both historical data (for daily base process) and streaming data (for incremental updates).
"""

import argparse
import csv
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
import pandas as pd

class ShipmentDataGenerator:
    """Generates realistic shipment event data for testing."""
    
    def __init__(self, seed: int = 42):
        """Initialize with random seed for reproducible data."""
        random.seed(seed)
        
        # Sample locations (simplified)
        self.locations = [
            {'id': 'FC001', 'name': 'Seattle Fulfillment Center', 'type': 'fulfillment_center', 'region': 'US-West'},
            {'id': 'FC002', 'name': 'Phoenix Fulfillment Center', 'type': 'fulfillment_center', 'region': 'US-West'},
            {'id': 'SC001', 'name': 'Denver Sort Center', 'type': 'sort_center', 'region': 'US-Central'},
            {'id': 'SC002', 'name': 'Chicago Sort Center', 'type': 'sort_center', 'region': 'US-Central'},
            {'id': 'DS001', 'name': 'Portland Delivery Station', 'type': 'delivery_station', 'region': 'US-West'},
            {'id': 'DS002', 'name': 'Sacramento Delivery Station', 'type': 'delivery_station', 'region': 'US-West'},
            {'id': 'DS003', 'name': 'Austin Delivery Station', 'type': 'delivery_station', 'region': 'US-Central'},
            {'id': 'DS004', 'name': 'Atlanta Delivery Station', 'type': 'delivery_station', 'region': 'US-East'},
        ]
        
        # Event types and typical progression
        self.event_progression = [
            ('created', 'Package created in system'),
            ('picked', 'Package picked from fulfillment center'),
            ('packed', 'Package packed and labeled'),
            ('shipped', 'Package shipped from fulfillment center'),
            ('in_transit', 'Package in transit between facilities'),
            ('arrived_sort', 'Package arrived at sort facility'),
            ('departed_sort', 'Package departed sort facility'),
            ('arrived_delivery', 'Package arrived at delivery station'),
            ('out_for_delivery', 'Package out for delivery'),
            ('delivered', 'Package delivered to customer'),
        ]
        
        # Carriers
        self.carriers = ['AMZL', 'UPS', 'FEDEX', 'DHL', 'USPS']
        
        # Service levels
        self.service_levels = ['standard', 'expedited', 'priority', 'same_day']
        
    def generate_shipment_timeline(self, shipment_id: str, start_time: datetime, 
                                 service_level: str = 'standard') -> List[Dict[str, Any]]:
        """Generate a complete timeline for a single shipment."""
        events = []
        current_time = start_time
        current_location_idx = 0
        
        # Determine delivery speed based on service level
        speed_multiplier = {
            'same_day': 0.1,
            'priority': 0.3,
            'expedited': 0.6,
            'standard': 1.0
        }.get(service_level, 1.0)
        
        # Generate events following typical progression
        for i, (event_type, description) in enumerate(self.event_progression):
            # Skip some events randomly (not all shipments follow exact same path)
            if random.random() < 0.1 and event_type not in ['created', 'delivered']:
                continue
                
            # Determine location based on event type
            if event_type in ['created', 'picked', 'packed', 'shipped']:
                location = self.locations[0]  # Start at fulfillment center
            elif event_type in ['arrived_sort', 'departed_sort']:
                location = self.locations[2 + random.randint(0, 1)]  # Sort centers
            elif event_type in ['arrived_delivery', 'out_for_delivery', 'delivered']:
                location = self.locations[4 + random.randint(0, 3)]  # Delivery stations
            else:
                location = random.choice(self.locations)
            
            # Time progression (with some randomness)
            if i == 0:
                time_delta = timedelta(minutes=0)  # Creation event
            else:
                base_hours = {
                    'picked': 2, 'packed': 1, 'shipped': 0.5,
                    'in_transit': 8, 'arrived_sort': 12, 'departed_sort': 2,
                    'arrived_delivery': 6, 'out_for_delivery': 2, 'delivered': 4
                }.get(event_type, 4)
                
                # Add randomness and apply speed multiplier
                actual_hours = base_hours * speed_multiplier * random.uniform(0.5, 2.0)
                time_delta = timedelta(hours=actual_hours)
            
            current_time += time_delta
            
            # Create event record
            event = {
                'shipment_id': shipment_id,
                'package_id': f"{shipment_id}_PKG",
                'event_timestamp': current_time,
                'event_type': event_type,
                'status': event_type,
                'location_id': location['id'],
                'location_name': location['name'],
                'facility_type': location['type'],
                'region': location['region'],
                'scan_type': 'automatic' if random.random() > 0.2 else 'manual',
                'carrier_code': random.choice(self.carriers),
                'service_level': service_level,
                'package_weight': round(random.uniform(0.5, 25.0), 2),
                'package_dimensions': f"{random.randint(6,20)}x{random.randint(4,15)}x{random.randint(2,10)}",
                'created_date': start_time.date(),
                'updated_date': current_time.date(),
                'description': description
            }
            
            events.append(event)
            
            # Stop if delivered
            if event_type == 'delivered':
                break
                
        return events
    
    def generate_historical_data(self, days: int = 10, shipments_per_day: int = 1000) -> List[Dict[str, Any]]:
        """Generate historical data for daily base process testing."""
        all_events = []
        
        # Generate data for each day
        for day_offset in range(days):
            day_start = datetime.now(timezone.utc) - timedelta(days=days-day_offset)
            
            # Generate shipments for this day
            for ship_idx in range(shipments_per_day):
                shipment_id = f"SHIP_{day_start.strftime('%Y%m%d')}_{ship_idx:04d}"
                
                # Random start time within the day
                start_offset = timedelta(hours=random.uniform(0, 24))
                start_time = day_start + start_offset
                
                # Random service level
                service_level = random.choice(self.service_levels)
                
                # Generate timeline for this shipment
                timeline = self.generate_shipment_timeline(shipment_id, start_time, service_level)
                all_events.extend(timeline)
                
        return all_events
    
    def generate_streaming_data(self, hours: int = 1, events_per_hour: int = 500) -> List[Dict[str, Any]]:
        """Generate streaming data for incremental update testing."""
        all_events = []
        
        # Start from current time
        base_time = datetime.now(timezone.utc)
        
        # Generate events distributed over the time period
        for event_idx in range(events_per_hour * hours):
            # Random time within the period
            time_offset = timedelta(hours=random.uniform(0, hours))
            event_time = base_time + time_offset
            
            # Could be new shipment or update to existing
            if random.random() < 0.3:  # 30% new shipments
                shipment_id = f"SHIP_NEW_{event_time.strftime('%Y%m%d_%H%M%S')}_{event_idx:04d}"
                service_level = random.choice(self.service_levels)
                timeline = self.generate_shipment_timeline(shipment_id, event_time, service_level)
                # Only include first few events for new shipments
                all_events.extend(timeline[:random.randint(1, 3)])
            else:  # Update to existing shipment
                # Simulate update to existing shipment (would need existing shipment IDs in real scenario)
                shipment_id = f"SHIP_EXISTING_{random.randint(1, 1000):04d}"
                location = random.choice(self.locations)
                event_type = random.choice([et[0] for et in self.event_progression[2:8]])  # Mid-timeline events
                
                event = {
                    'shipment_id': shipment_id,
                    'package_id': f"{shipment_id}_PKG",
                    'event_timestamp': event_time,
                    'event_type': event_type,
                    'status': event_type,
                    'location_id': location['id'],
                    'location_name': location['name'],
                    'facility_type': location['type'],
                    'region': location['region'],
                    'scan_type': 'automatic' if random.random() > 0.2 else 'manual',
                    'carrier_code': random.choice(self.carriers),
                    'service_level': random.choice(self.service_levels),
                    'package_weight': round(random.uniform(0.5, 25.0), 2),
                    'package_dimensions': f"{random.randint(6,20)}x{random.randint(4,15)}x{random.randint(2,10)}",
                    'created_date': (event_time - timedelta(days=random.randint(0, 5))).date(),
                    'updated_date': event_time.date(),
                    'description': f"Update event: {event_type}"
                }
                
                all_events.append(event)
                
        return all_events
    
    def save_to_csv(self, events: List[Dict[str, Any]], filename: str):
        """Save events to CSV file."""
        if not events:
            print(f"No events to save to {filename}")
            return
            
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=events[0].keys())
            writer.writeheader()
            for event in events:
                # Convert datetime to string for CSV
                row = event.copy()
                row['event_timestamp'] = row['event_timestamp'].isoformat()
                writer.writerow(row)
        
        print(f"Saved {len(events)} events to {filename}")
    
    def save_to_json(self, events: List[Dict[str, Any]], filename: str):
        """Save events to JSON file."""
        if not events:
            print(f"No events to save to {filename}")
            return
            
        # Convert datetime objects to strings for JSON serialization
        json_events = []
        for event in events:
            json_event = event.copy()
            json_event['event_timestamp'] = json_event['event_timestamp'].isoformat()
            json_events.append(json_event)
        
        with open(filename, 'w') as jsonfile:
            json.dump(json_events, jsonfile, indent=2)
        
        print(f"Saved {len(events)} events to {filename}")
    
    def generate_s3_event_sample(self, bucket: str = 'shipment-data-bucket', 
                                key: str = 'shipment-events/2025/07/11/events.json') -> Dict[str, Any]:
        """Generate a sample S3 event for testing incremental triggers."""
        return {
            "Records": [{
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": datetime.now(timezone.utc).isoformat(),
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "shipment-event-trigger",
                    "bucket": {
                        "name": bucket,
                        "arn": f"arn:aws:s3:::{bucket}"
                    },
                    "object": {
                        "key": key,
                        "size": random.randint(10000, 100000),
                        "eTag": f"{uuid.uuid4().hex}",
                        "sequencer": f"{random.randint(100000000000000, 999999999999999):015X}"
                    }
                }
            }]
        }


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description='Generate sample shipment timeline data')
    parser.add_argument('--type', required=True, choices=['historical', 'streaming', 's3-event'],
                       help='Type of data to generate')
    parser.add_argument('--output', required=True, help='Output filename')
    parser.add_argument('--format', choices=['csv', 'json'], default='json', help='Output format')
    parser.add_argument('--days', type=int, default=10, help='Days of historical data')
    parser.add_argument('--shipments-per-day', type=int, default=1000, help='Shipments per day')
    parser.add_argument('--hours', type=int, default=1, help='Hours of streaming data')
    parser.add_argument('--events-per-hour', type=int, default=500, help='Events per hour for streaming')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducible data')
    
    args = parser.parse_args()
    
    generator = ShipmentDataGenerator(seed=args.seed)
    
    if args.type == 'historical':
        print(f"Generating {args.days} days of historical data...")
        events = generator.generate_historical_data(args.days, args.shipments_per_day)
        
        if args.format == 'csv':
            generator.save_to_csv(events, args.output)
        else:
            generator.save_to_json(events, args.output)
            
    elif args.type == 'streaming':
        print(f"Generating {args.hours} hours of streaming data...")
        events = generator.generate_streaming_data(args.hours, args.events_per_hour)
        
        if args.format == 'csv':
            generator.save_to_csv(events, args.output)
        else:
            generator.save_to_json(events, args.output)
            
    elif args.type == 's3-event':
        print("Generating sample S3 event...")
        s3_event = generator.generate_s3_event_sample()
        
        with open(args.output, 'w') as f:
            json.dump(s3_event, f, indent=2)
        
        print(f"Saved S3 event sample to {args.output}")

if __name__ == '__main__':
    main()