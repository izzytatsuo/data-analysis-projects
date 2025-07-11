#!/usr/bin/env python3
"""
Test Script for Shipment Timeline Workflow

Demonstrates the complete workflow with sample data:
1. Generate sample data
2. Run daily base process
3. Generate incremental data
4. Run incremental update
5. Query real-time view
6. Show performance metrics
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
import tempfile
import subprocess

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from demo.sample_data_generator import ShipmentDataGenerator

class WorkflowTester:
    """Test the complete shipment timeline workflow."""
    
    def __init__(self):
        self.generator = ShipmentDataGenerator()
        self.temp_dir = tempfile.mkdtemp()
        print(f"Using temporary directory: {self.temp_dir}")
        
    def run_complete_test(self):
        """Run the complete workflow test."""
        print("="*60)
        print("SHIPMENT TIMELINE WORKFLOW - COMPLETE TEST")
        print("="*60)
        
        try:
            # Step 1: Generate historical data
            print("\n1. Generating Historical Data (Daily Base)")
            print("-" * 40)
            historical_events = self.generator.generate_historical_data(days=10, shipments_per_day=100)
            historical_file = os.path.join(self.temp_dir, "historical_events.json")
            self.generator.save_to_json(historical_events, historical_file)
            
            self.analyze_dataset("Historical", historical_events)
            
            # Step 2: Simulate daily base process
            print("\n2. Daily Base Process Simulation")
            print("-" * 40)
            self.simulate_daily_base_process(historical_events)
            
            # Step 3: Generate incremental data
            print("\n3. Generating Incremental Data")
            print("-" * 40)
            incremental_events = self.generator.generate_streaming_data(hours=2, events_per_hour=50)
            incremental_file = os.path.join(self.temp_dir, "incremental_events.json")
            self.generator.save_to_json(incremental_events, incremental_file)
            
            self.analyze_dataset("Incremental", incremental_events)
            
            # Step 4: Simulate incremental process
            print("\n4. Incremental Update Process Simulation")
            print("-" * 40)
            self.simulate_incremental_process(incremental_events)
            
            # Step 5: Simulate real-time view
            print("\n5. Real-time View Simulation")
            print("-" * 40)
            self.simulate_realtime_view(historical_events, incremental_events)
            
            # Step 6: Performance analysis
            print("\n6. Performance Analysis")
            print("-" * 40)
            self.analyze_performance()
            
            # Step 7: Generate S3 event sample
            print("\n7. S3 Event Trigger Sample")
            print("-" * 40)
            self.generate_s3_event_sample()
            
            print("\n" + "="*60)
            print("TEST COMPLETED SUCCESSFULLY!")
            print("="*60)
            
        except Exception as e:
            print(f"\nTest failed with error: {e}")
            raise
    
    def analyze_dataset(self, dataset_name: str, events: list):
        """Analyze and summarize a dataset."""
        if not events:
            print(f"  {dataset_name} dataset is empty")
            return
            
        shipments = set(event['shipment_id'] for event in events)
        event_types = {}
        locations = set()
        carriers = set()
        
        earliest_time = min(event['event_timestamp'] for event in events)
        latest_time = max(event['event_timestamp'] for event in events)
        
        for event in events:
            event_type = event['event_type']
            event_types[event_type] = event_types.get(event_type, 0) + 1
            locations.add(event['location_id'])
            carriers.add(event['carrier_code'])
        
        print(f"  📊 {dataset_name} Dataset Summary:")
        print(f"    • Total Events: {len(events):,}")
        print(f"    • Unique Shipments: {len(shipments):,}")
        print(f"    • Time Range: {earliest_time} to {latest_time}")
        print(f"    • Locations: {len(locations)} ({', '.join(sorted(locations))})")
        print(f"    • Carriers: {len(carriers)} ({', '.join(sorted(carriers))})")
        print(f"    • Top Event Types:")
        for event_type, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"      - {event_type}: {count:,}")
    
    def simulate_daily_base_process(self, events: list):
        """Simulate the daily base processing."""
        start_time = time.time()
        
        print("  🕛 Simulating Daily Base Process...")
        
        # Simulate processing steps
        steps = [
            ("Extract 10 days of data", 0.5),
            ("Enrich with location/package data", 1.0),
            ("Compute window functions", 3.0),
            ("Create timeline with sequences", 2.0),
            ("Store base timeline", 1.5),
            ("Update checkpoint", 0.5),
            ("Create indexes", 1.0)
        ]
        
        for step_name, duration in steps:
            print(f"    ⏳ {step_name}...")
            time.sleep(duration * 0.1)  # Simulate processing time (scaled down)
            print(f"    ✅ {step_name} completed")
        
        end_time = time.time()
        
        # Simulate results
        shipments = set(event['shipment_id'] for event in events)
        delivered_count = sum(1 for event in events if event['event_type'] == 'delivered')
        
        print(f"\n  📋 Daily Base Process Results:")
        print(f"    • Processing Time: {end_time - start_time:.2f} seconds (simulated)")
        print(f"    • Records Processed: {len(events):,}")
        print(f"    • Unique Shipments: {len(shipments):,}")
        print(f"    • Delivered Shipments: {delivered_count:,}")
        print(f"    • Checkpoint Updated: {datetime.now(timezone.utc).isoformat()}")
        
    def simulate_incremental_process(self, events: list):
        """Simulate the incremental update processing."""
        start_time = time.time()
        
        print("  ⚡ Simulating Incremental Update Process...")
        
        # Simulate processing steps
        steps = [
            ("Get current checkpoint", 0.1),
            ("Extract new events since checkpoint", 0.3),
            ("Get context from base timeline", 0.5),
            ("Compute window functions for new events", 1.0),
            ("Append to incremental timeline", 0.3),
            ("Update checkpoint", 0.1)
        ]
        
        for step_name, duration in steps:
            print(f"    ⏳ {step_name}...")
            time.sleep(duration * 0.1)  # Simulate processing time (scaled down)
            print(f"    ✅ {step_name} completed")
        
        end_time = time.time()
        
        # Simulate results
        new_shipments = len(set(event['shipment_id'] for event in events if 'NEW' in event['shipment_id']))
        existing_updates = len(events) - new_shipments
        
        print(f"\n  📋 Incremental Update Results:")
        print(f"    • Processing Time: {end_time - start_time:.2f} seconds (simulated)")
        print(f"    • New Events Processed: {len(events):,}")
        print(f"    • New Shipments: {new_shipments:,}")
        print(f"    • Updates to Existing: {existing_updates:,}")
        print(f"    • Checkpoint Updated: {datetime.now(timezone.utc).isoformat()}")
    
    def simulate_realtime_view(self, base_events: list, incremental_events: list):
        """Simulate the real-time view combination."""
        print("  🎯 Simulating Real-time View Generation...")
        
        # Combine datasets
        all_events = base_events + incremental_events
        
        # Simulate view creation steps
        steps = [
            ("Union base and incremental data", 0.2),
            ("Correct event sequences across boundary", 0.5),
            ("Recalculate derived fields", 0.3),
            ("Refresh materialized view", 0.4),
            ("Update view indexes", 0.2)
        ]
        
        for step_name, duration in steps:
            print(f"    ⏳ {step_name}...")
            time.sleep(duration * 0.1)
            print(f"    ✅ {step_name} completed")
        
        # Simulate query examples
        print(f"\n  🔍 Sample Real-time Queries:")
        
        # Active shipments
        active_shipments = set()
        delivered_shipments = set()
        for event in all_events:
            if event['event_type'] == 'delivered':
                delivered_shipments.add(event['shipment_id'])
            else:
                active_shipments.add(event['shipment_id'])
        
        active_shipments -= delivered_shipments  # Remove delivered from active
        
        print(f"    • Active Shipments: {len(active_shipments):,}")
        print(f"    • Delivered Shipments: {len(delivered_shipments):,}")
        
        # Location distribution
        location_counts = {}
        for event in all_events:
            if event['shipment_id'] in active_shipments:
                loc = event['location_id']
                location_counts[loc] = location_counts.get(loc, 0) + 1
        
        print(f"    • Active Shipments by Location:")
        for loc, count in sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:3]:
            print(f"      - {loc}: {count:,}")
    
    def analyze_performance(self):
        """Analyze and display performance metrics."""
        print("  📈 Performance Analysis:")
        
        # Simulated performance metrics
        traditional_metrics = {
            'daily_processing_hours': 6.0,
            'intraday_update_hours': 4.0,
            'daily_compute_cost': 400,
            'data_freshness_hours': 5.0
        }
        
        incremental_metrics = {
            'daily_processing_hours': 0.5,
            'intraday_update_hours': 0.083,  # 5 minutes
            'daily_compute_cost': 50,
            'data_freshness_hours': 0.167   # 10 minutes
        }
        
        print(f"\n    Traditional vs Incremental Approach:")
        print(f"    {'Metric':<25} {'Traditional':<12} {'Incremental':<12} {'Improvement':<12}")
        print(f"    {'-'*25} {'-'*12} {'-'*12} {'-'*12}")
        
        metrics = [
            ('Daily Processing', 'daily_processing_hours', 'hours'),
            ('Intraday Updates', 'intraday_update_hours', 'hours'),
            ('Daily Compute Cost', 'daily_compute_cost', '$'),
            ('Data Freshness', 'data_freshness_hours', 'hours'),
        ]
        
        for name, key, unit in metrics:
            traditional = traditional_metrics[key]
            incremental = incremental_metrics[key]
            
            if traditional > 0:
                improvement = (traditional - incremental) / traditional * 100
                improvement_str = f"{improvement:.1f}% better"
            else:
                improvement_str = "N/A"
            
            print(f"    {name:<25} {traditional:>8.2f} {unit:<3} {incremental:>8.2f} {unit:<3} {improvement_str}")
        
        # Cost savings calculation
        daily_savings = traditional_metrics['daily_compute_cost'] - incremental_metrics['daily_compute_cost']
        annual_savings = daily_savings * 365
        
        print(f"\n    💰 Cost Savings:")
        print(f"    • Daily Savings: ${daily_savings}")
        print(f"    • Annual Savings: ${annual_savings:,}")
    
    def generate_s3_event_sample(self):
        """Generate and save S3 event sample."""
        s3_event = self.generator.generate_s3_event_sample()
        s3_event_file = os.path.join(self.temp_dir, "s3_event_sample.json")
        
        with open(s3_event_file, 'w') as f:
            json.dump(s3_event, f, indent=2)
        
        print(f"  📁 S3 Event Sample Generated:")
        print(f"    • File: {s3_event_file}")
        print(f"    • Bucket: {s3_event['Records'][0]['s3']['bucket']['name']}")
        print(f"    • Key: {s3_event['Records'][0]['s3']['object']['key']}")
        print(f"    • Size: {s3_event['Records'][0]['s3']['object']['size']} bytes")
        
        print(f"\n    💡 Usage Example:")
        print(f"    python workflow_orchestrator.py --mode incremental --s3-event-file {s3_event_file}")
    
    def generate_sample_files(self):
        """Generate sample files for manual testing."""
        print("\n" + "="*60)
        print("GENERATING SAMPLE FILES FOR MANUAL TESTING")
        print("="*60)
        
        # Generate different sized datasets
        datasets = [
            ("small", {"days": 3, "shipments_per_day": 50}),
            ("medium", {"days": 7, "shipments_per_day": 200}),
            ("large", {"days": 10, "shipments_per_day": 1000})
        ]
        
        for size, params in datasets:
            print(f"\nGenerating {size} dataset...")
            events = self.generator.generate_historical_data(**params)
            
            # Save in multiple formats
            csv_file = os.path.join(self.temp_dir, f"shipment_events_{size}.csv")
            json_file = os.path.join(self.temp_dir, f"shipment_events_{size}.json")
            
            self.generator.save_to_csv(events, csv_file)
            self.generator.save_to_json(events, json_file)
            
            print(f"  Saved {size} dataset: {len(events):,} events")
        
        # Generate streaming samples
        streaming_events = self.generator.generate_streaming_data(hours=1, events_per_hour=100)
        streaming_file = os.path.join(self.temp_dir, "streaming_events.json")
        self.generator.save_to_json(streaming_events, streaming_file)
        
        print(f"\nGenerated streaming sample: {len(streaming_events):,} events")
        print(f"All sample files saved to: {self.temp_dir}")


def main():
    """Main function."""
    print("Shipment Timeline Workflow Tester")
    print("=" * 40)
    
    tester = WorkflowTester()
    
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--generate-samples":
        tester.generate_sample_files()
    else:
        tester.run_complete_test()
    
    print(f"\nTemporary files created in: {tester.temp_dir}")
    print("You can examine the generated data files and use them for testing.")

if __name__ == '__main__':
    main()