#!/usr/bin/env python3
"""
Shipment Timeline Workflow Orchestrator

This script manages the two-stage incremental processing pipeline:
1. Daily base process (midnight UTC)
2. Event-driven incremental updates (triggered by S3 events)

Usage:
    python workflow_orchestrator.py --mode daily_base
    python workflow_orchestrator.py --mode incremental --s3-event-file event.json
    python workflow_orchestrator.py --mode refresh_view
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import psycopg2
import boto3
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ShipmentTimelineOrchestrator:
    """Orchestrates the shipment timeline processing workflow."""
    
    def __init__(self, db_config: Dict[str, str], aws_config: Optional[Dict[str, str]] = None):
        """
        Initialize the orchestrator.
        
        Args:
            db_config: Database connection configuration
            aws_config: AWS configuration for S3 and other services
        """
        self.db_config = db_config
        self.aws_config = aws_config or {}
        self.db_connection = None
        self.s3_client = None
        
        # SQL file paths
        self.sql_files = {
            'daily_base': 'daily_base_process.sql',
            'incremental': 'incremental_update_process.sql',
            'realtime_view': 'realtime_timeline_view.sql'
        }
        
    def connect_database(self) -> psycopg2.connection:
        """Establish database connection."""
        if not self.db_connection or self.db_connection.closed:
            try:
                self.db_connection = psycopg2.connect(**self.db_config)
                self.db_connection.autocommit = False
                logger.info("Database connection established")
            except Exception as e:
                logger.error(f"Failed to connect to database: {e}")
                raise
        return self.db_connection
    
    def connect_s3(self) -> boto3.client:
        """Establish S3 client connection."""
        if not self.s3_client:
            try:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_config.get('access_key_id'),
                    aws_secret_access_key=self.aws_config.get('secret_access_key'),
                    region_name=self.aws_config.get('region', 'us-east-1')
                )
                logger.info("S3 client connection established")
            except Exception as e:
                logger.error(f"Failed to connect to S3: {e}")
                raise
        return self.s3_client
    
    def execute_sql_file(self, sql_file: str, parameters: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Execute SQL commands from a file.
        
        Args:
            sql_file: Path to SQL file
            parameters: Optional parameters to substitute in SQL
            
        Returns:
            Execution results and statistics
        """
        conn = self.connect_database()
        start_time = time.time()
        
        try:
            # Read SQL file
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            # Substitute parameters if provided
            if parameters:
                for key, value in parameters.items():
                    sql_content = sql_content.replace(f'${{{key}}}', str(value))
            
            # Execute SQL
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql_content)
                
                # Try to fetch results (for SELECT statements)
                results = []
                try:
                    results = cursor.fetchall()
                except psycopg2.ProgrammingError:
                    # No results to fetch (e.g., INSERT, UPDATE, CREATE)
                    pass
                
                conn.commit()
                execution_time = time.time() - start_time
                
                logger.info(f"Successfully executed {sql_file} in {execution_time:.2f} seconds")
                
                return {
                    'success': True,
                    'execution_time': execution_time,
                    'results': results,
                    'row_count': len(results) if results else cursor.rowcount
                }
                
        except Exception as e:
            conn.rollback()
            execution_time = time.time() - start_time
            logger.error(f"Failed to execute {sql_file}: {e}")
            
            return {
                'success': False,
                'execution_time': execution_time,
                'error': str(e),
                'results': []
            }
    
    def run_daily_base_process(self) -> Dict[str, Any]:
        """
        Run the daily base processing workflow.
        
        This should be scheduled to run at midnight UTC daily.
        """
        logger.info("Starting daily base process")
        
        # Check if today's base process already completed
        if self.is_daily_base_completed():
            logger.info("Daily base process already completed for today")
            return {'success': True, 'message': 'Already completed'}
        
        # Execute daily base SQL
        result = self.execute_sql_file(self.sql_files['daily_base'])
        
        if result['success']:
            # Refresh the real-time view
            self.refresh_realtime_view()
            
            # Send notification/alert about completion
            self.send_processing_notification('daily_base', result)
            
            logger.info("Daily base process completed successfully")
        else:
            logger.error(f"Daily base process failed: {result.get('error')}")
            self.send_error_notification('daily_base', result)
        
        return result
    
    def run_incremental_update(self, s3_event: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Run incremental update process.
        
        Args:
            s3_event: S3 event that triggered this update (optional)
        """
        logger.info("Starting incremental update process")
        
        # Validate that daily base exists
        if not self.is_daily_base_available():
            logger.error("Cannot run incremental update: daily base not available")
            return {
                'success': False,
                'error': 'Daily base process must be completed first'
            }
        
        # Process S3 event if provided
        if s3_event:
            self.process_s3_event(s3_event)
        
        # Execute incremental update SQL
        result = self.execute_sql_file(self.sql_files['incremental'])
        
        if result['success']:
            # Refresh the real-time view (lightweight refresh)
            self.refresh_realtime_view()
            
            # Update monitoring metrics
            self.update_processing_metrics('incremental', result)
            
            logger.info("Incremental update completed successfully")
        else:
            logger.error(f"Incremental update failed: {result.get('error')}")
            self.send_error_notification('incremental', result)
        
        return result
    
    def refresh_realtime_view(self) -> Dict[str, Any]:
        """Refresh the real-time materialized view."""
        logger.info("Refreshing real-time view")
        
        conn = self.connect_database()
        start_time = time.time()
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT refresh_shipment_timeline_realtime();")
                conn.commit()
                
                execution_time = time.time() - start_time
                logger.info(f"Real-time view refreshed in {execution_time:.2f} seconds")
                
                return {
                    'success': True,
                    'execution_time': execution_time
                }
                
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to refresh real-time view: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def is_daily_base_completed(self) -> bool:
        """Check if today's daily base process is already completed."""
        conn = self.connect_database()
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT status, last_processed_timestamp 
                    FROM processing_checkpoints 
                    WHERE checkpoint_id = 'shipment_timeline' 
                        AND process_type = 'daily_base'
                        AND DATE(last_processed_timestamp) = CURRENT_DATE
                        AND status = 'completed'
                """)
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            logger.error(f"Error checking daily base status: {e}")
            return False
    
    def is_daily_base_available(self) -> bool:
        """Check if a valid daily base exists for incremental processing."""
        conn = self.connect_database()
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM shipment_timeline_base
                """)
                base_count = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT status FROM processing_checkpoints 
                    WHERE checkpoint_id = 'shipment_timeline' 
                        AND process_type = 'daily_base'
                        AND status = 'completed'
                """)
                checkpoint_status = cursor.fetchone()
                
                return base_count > 0 and checkpoint_status is not None
                
        except Exception as e:
            logger.error(f"Error checking daily base availability: {e}")
            return False
    
    def process_s3_event(self, s3_event: Dict) -> None:
        """Process S3 event to understand what data arrived."""
        try:
            # Extract relevant information from S3 event
            bucket = s3_event.get('bucket', {}).get('name')
            key = s3_event.get('object', {}).get('key')
            size = s3_event.get('object', {}).get('size', 0)
            
            logger.info(f"Processing S3 event: s3://{bucket}/{key} ({size} bytes)")
            
            # Could add logic here to:
            # - Validate the file format
            # - Check if it's a file we care about
            # - Extract timestamp information from filename
            # - Update processing metadata
            
        except Exception as e:
            logger.error(f"Error processing S3 event: {e}")
    
    def send_processing_notification(self, process_type: str, result: Dict) -> None:
        """Send notification about successful processing."""
        # Implement notification logic (SNS, email, Slack, etc.)
        logger.info(f"Processing notification for {process_type}: {result}")
    
    def send_error_notification(self, process_type: str, result: Dict) -> None:
        """Send error notification."""
        # Implement error notification logic
        logger.error(f"Error notification for {process_type}: {result}")
    
    def update_processing_metrics(self, process_type: str, result: Dict) -> None:
        """Update processing metrics for monitoring."""
        # Could send metrics to CloudWatch, Datadog, etc.
        logger.info(f"Updating metrics for {process_type}: {result}")
    
    def get_processing_status(self) -> Dict[str, Any]:
        """Get current processing status and health metrics."""
        conn = self.connect_database()
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get checkpoint status
                cursor.execute("""
                    SELECT * FROM processing_checkpoints 
                    WHERE checkpoint_id = 'shipment_timeline'
                """)
                checkpoint = cursor.fetchone()
                
                # Get recent processing stats
                cursor.execute("""
                    SELECT * FROM processing_stats 
                    WHERE checkpoint_id = 'shipment_timeline'
                    ORDER BY process_date DESC, last_update_time DESC
                    LIMIT 10
                """)
                recent_stats = cursor.fetchall()
                
                # Get timeline health
                cursor.execute("""
                    SELECT * FROM timeline_processing_health
                """)
                health = cursor.fetchone()
                
                return {
                    'checkpoint': dict(checkpoint) if checkpoint else None,
                    'recent_stats': [dict(stat) for stat in recent_stats],
                    'health': dict(health) if health else None,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error getting processing status: {e}")
            return {'error': str(e)}


def main():
    """Main entry point for the orchestrator."""
    parser = argparse.ArgumentParser(description='Shipment Timeline Workflow Orchestrator')
    parser.add_argument('--mode', required=True, 
                       choices=['daily_base', 'incremental', 'refresh_view', 'status'],
                       help='Processing mode')
    parser.add_argument('--s3-event-file', help='Path to S3 event JSON file')
    parser.add_argument('--config', help='Path to configuration file')
    
    args = parser.parse_args()
    
    # Load configuration
    config = {
        'database': {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'shipment_data'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        },
        'aws': {
            'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'region': os.getenv('AWS_REGION', 'us-east-1')
        }
    }
    
    # Initialize orchestrator
    orchestrator = ShipmentTimelineOrchestrator(
        db_config=config['database'],
        aws_config=config['aws']
    )
    
    # Execute based on mode
    try:
        if args.mode == 'daily_base':
            result = orchestrator.run_daily_base_process()
            
        elif args.mode == 'incremental':
            s3_event = None
            if args.s3_event_file:
                with open(args.s3_event_file, 'r') as f:
                    s3_event = json.load(f)
            result = orchestrator.run_incremental_update(s3_event)
            
        elif args.mode == 'refresh_view':
            result = orchestrator.refresh_realtime_view()
            
        elif args.mode == 'status':
            result = orchestrator.get_processing_status()
            print(json.dumps(result, indent=2, default=str))
            return
        
        # Output result
        if result['success']:
            logger.info(f"Operation completed successfully: {result}")
            sys.exit(0)
        else:
            logger.error(f"Operation failed: {result}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()