#!/usr/bin/env python3
"""
Mermaid Diagram Generator for SQL Workflows

Creates interactive Mermaid.js diagrams showing:
- Table relationships (ERD)
- Query execution flows
- Data pipeline architecture
- Performance metrics visualization
"""

import json
import os
from typing import Dict, List, Any

class MermaidSQLVisualizer:
    """Generate Mermaid diagrams for SQL workflows."""
    
    def __init__(self):
        self.diagrams = {}
        
    def create_table_relationship_diagram(self) -> str:
        """Create ERD showing table relationships."""
        return """
erDiagram
    SHIPMENT_EVENTS {
        varchar shipment_id PK
        timestamp event_timestamp
        varchar event_type
        varchar location_id FK
        varchar package_id FK
        varchar status
        varchar carrier_code
    }
    
    LOCATIONS {
        varchar location_id PK
        varchar location_name
        varchar region
        varchar facility_type
        varchar country_code
    }
    
    PACKAGES {
        varchar package_id PK
        decimal package_weight
        varchar package_dimensions
        varchar service_level
    }
    
    PROCESSING_CHECKPOINTS {
        varchar checkpoint_id PK
        timestamp last_processed_timestamp
        varchar process_type
        varchar status
    }
    
    SHIPMENT_TIMELINE_BASE {
        varchar shipment_id PK
        timestamp event_timestamp PK
        integer event_sequence
        timestamp prev_event_timestamp
        interval time_since_prev_event
        decimal cumulative_transit_hours
        varchar data_source
    }
    
    SHIPMENT_TIMELINE_INCREMENTAL {
        varchar shipment_id PK
        timestamp event_timestamp PK
        integer event_sequence
        uuid incremental_batch_id
        timestamp incremental_processed_at
    }
    
    SHIPMENT_TIMELINE_REALTIME {
        varchar shipment_id PK
        timestamp event_timestamp PK
        integer corrected_sequence
        varchar source_type
        boolean is_latest_event
    }
    
    %% Relationships
    SHIPMENT_EVENTS ||--o{ LOCATIONS : "located_at"
    SHIPMENT_EVENTS ||--o{ PACKAGES : "contains"
    SHIPMENT_EVENTS ||--o{ SHIPMENT_TIMELINE_BASE : "processed_into"
    SHIPMENT_EVENTS ||--o{ SHIPMENT_TIMELINE_INCREMENTAL : "updated_into"
    SHIPMENT_TIMELINE_BASE ||--o{ SHIPMENT_TIMELINE_REALTIME : "combined_into"
    SHIPMENT_TIMELINE_INCREMENTAL ||--o{ SHIPMENT_TIMELINE_REALTIME : "combined_into"
    PROCESSING_CHECKPOINTS ||--o{ SHIPMENT_TIMELINE_INCREMENTAL : "controls"
"""

    def create_daily_base_flow(self) -> str:
        """Create flowchart for daily base processing."""
        return """
flowchart TD
    A[🕛 Midnight UTC Trigger] --> B[📊 Extract 10 Days Data]
    B --> C{Validate Data Quality}
    C -->|✅ Valid| D[🔧 Enrich with Locations]
    C -->|❌ Invalid| E[📧 Alert Data Team]
    E --> F[🛑 Stop Process]
    
    D --> G[📦 Enrich with Packages]
    G --> H[🪟 Compute Window Functions]
    H --> I[Event Sequencing]
    H --> J[Time Calculations]
    H --> K[Location Transitions]
    
    I --> L[💾 Store Base Timeline]
    J --> L
    K --> L
    
    L --> M[📈 Create Indexes]
    M --> N[✅ Update Checkpoint]
    N --> O[📊 Generate Statistics]
    O --> P[🎯 Success]
    
    %% Styling
    classDef startEnd fill:#e1f5fe
    classDef process fill:#f3e5f5
    classDef decision fill:#fff3e0
    classDef storage fill:#e8f5e8
    classDef error fill:#ffebee
    
    class A,P startEnd
    class B,D,G,H,I,J,K,M,O process
    class C decision
    class L,N storage
    class E,F error
"""

    def create_incremental_flow(self) -> str:
        """Create flowchart for incremental processing."""
        return """
flowchart TD
    A[📁 S3 Event Trigger] --> B[⚡ Lambda Function]
    B --> C[🔍 Get Checkpoint]
    C --> D{Base Available?}
    D -->|❌ No| E[❌ Error: Run Daily Base First]
    D -->|✅ Yes| F[📊 Extract New Events]
    
    F --> G{New Events Found?}
    G -->|❌ No| H[ℹ️ No Processing Needed]
    G -->|✅ Yes| I[🧩 Get Existing Context]
    
    I --> J[🔧 Process New Events]
    J --> K[Continue Sequences]
    J --> L[Calculate Time Deltas]
    J --> M[Update Metrics]
    
    K --> N[📝 Append to Incremental]
    L --> N
    M --> N
    
    N --> O[🔄 Refresh Real-time View]
    O --> P[✅ Update Checkpoint]
    P --> Q[📊 Log Metrics]
    Q --> R[🎯 Success]
    
    %% Performance indicators
    S[⏱️ 2-5 minutes] -.-> J
    T[💰 $2-5 cost] -.-> J
    U[📈 1-5% dataset] -.-> F
    
    %% Styling
    classDef trigger fill:#e3f2fd
    classDef process fill:#f3e5f5
    classDef decision fill:#fff3e0
    classDef storage fill:#e8f5e8
    classDef error fill:#ffebee
    classDef metric fill:#fafafa
    
    class A,B trigger
    class C,F,I,J,K,L,M,O,Q process
    class D,G decision
    class N,P storage
    class E error
    class S,T,U metric
"""

    def create_performance_comparison(self) -> str:
        """Create chart comparing traditional vs incremental approach."""
        return """
%%{init: {'theme':'base', 'themeVariables': { 'primaryColor': '#ff0000'}}}%%
gitgraph
   commit id: "Traditional Approach"
   commit id: "Daily Process: 6 hours"
   commit id: "Update 1: 4 hours"
   commit id: "Update 2: 4 hours" 
   commit id: "Update 3: 4 hours"
   commit id: "Cost: $400/day"
   
   checkout main
   branch incremental
   commit id: "Incremental Approach"
   commit id: "Daily Base: 30 min"
   commit id: "Update 1: 5 min"
   commit id: "Update 2: 5 min"
   commit id: "Update 3: 5 min"
   commit id: "Cost: $50/day"
   commit id: "Savings: 95% improvement"
"""

    def create_data_lineage(self) -> str:
        """Create data lineage diagram."""
        return """
flowchart LR
    %% Data Sources
    API[📡 Shipment APIs] --> S3_RAW[🗄️ S3 Raw Data]
    STREAM[🌊 Event Streams] --> S3_RAW
    FILES[📄 File Uploads] --> S3_RAW
    
    %% Processing Pipeline
    S3_RAW --> DAILY[🕛 Daily Base Process]
    S3_RAW --> INCR[⚡ Incremental Process]
    
    %% Storage Layer
    DAILY --> BASE[(📊 Base Timeline)]
    INCR --> INCREMENTAL[(⚡ Incremental Timeline)]
    
    %% Reference Data
    LOC_API[📍 Locations API] --> LOC_REF[(📍 Locations)]
    PKG_API[📦 Package API] --> PKG_REF[(📦 Packages)]
    
    LOC_REF --> DAILY
    PKG_REF --> DAILY
    LOC_REF --> INCR
    PKG_REF --> INCR
    
    %% Output Views
    BASE --> REALTIME[🎯 Real-time View]
    INCREMENTAL --> REALTIME
    
    %% Query Layer
    REALTIME --> DASH[📈 Dashboards]
    REALTIME --> API_OUT[🔌 APIs]
    REALTIME --> REPORTS[📋 Reports]
    
    %% Monitoring
    DAILY --> METRICS[📊 Metrics]
    INCR --> METRICS
    METRICS --> ALERTS[🚨 Alerts]
    
    %% Styling
    classDef source fill:#e3f2fd
    classDef process fill:#f3e5f5
    classDef storage fill:#e8f5e8
    classDef output fill:#fff3e0
    classDef monitor fill:#fce4ec
    
    class API,STREAM,FILES,LOC_API,PKG_API source
    class DAILY,INCR process
    class S3_RAW,BASE,INCREMENTAL,LOC_REF,PKG_REF storage
    class REALTIME,DASH,API_OUT,REPORTS output
    class METRICS,ALERTS monitor
"""

    def create_html_wrapper(self, diagram_type: str, diagram_content: str, title: str) -> str:
        """Wrap Mermaid diagram in interactive HTML."""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f8f9fa;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .diagram-container {{
            padding: 30px;
            text-align: center;
        }}
        
        .controls {{
            padding: 20px;
            background: #f8f9fa;
            border-top: 1px solid #dee2e6;
            text-align: center;
        }}
        
        .control-btn {{
            padding: 10px 20px;
            margin: 0 10px;
            background: #007bff;
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.3s ease;
        }}
        
        .control-btn:hover {{
            background: #0056b3;
            transform: translateY(-2px);
        }}
        
        #diagram {{
            min-height: 400px;
            border: 1px solid #dee2e6;
            border-radius: 8px;
            background: white;
        }}
        
        .info-panel {{
            background: #f8f9fa;
            padding: 20px;
            margin: 20px 0;
            border-radius: 8px;
            border-left: 4px solid #007bff;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{title}</h1>
            <p>Interactive SQL Workflow Visualization</p>
        </div>
        
        <div class="diagram-container">
            <div id="diagram">
                <pre class="mermaid">
{diagram_content}
                </pre>
            </div>
            
            <div class="info-panel">
                <h3>Diagram Information</h3>
                <p>This interactive diagram shows the {diagram_type.replace('_', ' ')} for the shipment timeline workflow. 
                Click on elements to explore details, or use the controls below to modify the view.</p>
            </div>
        </div>
        
        <div class="controls">
            <button class="control-btn" onclick="zoomIn()">🔍 Zoom In</button>
            <button class="control-btn" onclick="zoomOut()">🔍 Zoom Out</button>
            <button class="control-btn" onclick="resetZoom()">🔄 Reset</button>
            <button class="control-btn" onclick="exportSVG()">💾 Export SVG</button>
            <button class="control-btn" onclick="exportPNG()">📷 Export PNG</button>
        </div>
    </div>
    
    <script>
        // Initialize Mermaid
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'default',
            flowchart: {{
                useMaxWidth: true,
                htmlLabels: true,
                curve: 'basis'
            }},
            er: {{
                useMaxWidth: true
            }}
        }});
        
        let currentZoom = 1;
        
        function zoomIn() {{
            currentZoom += 0.2;
            document.getElementById('diagram').style.transform = `scale(${{currentZoom}})`;
        }}
        
        function zoomOut() {{
            currentZoom = Math.max(0.2, currentZoom - 0.2);
            document.getElementById('diagram').style.transform = `scale(${{currentZoom}})`;
        }}
        
        function resetZoom() {{
            currentZoom = 1;
            document.getElementById('diagram').style.transform = 'scale(1)';
        }}
        
        function exportSVG() {{
            const svg = document.querySelector('#diagram svg');
            if (svg) {{
                const svgData = new XMLSerializer().serializeToString(svg);
                const blob = new Blob([svgData], {{type: 'image/svg+xml'}});
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = '{diagram_type}_diagram.svg';
                a.click();
            }}
        }}
        
        function exportPNG() {{
            const svg = document.querySelector('#diagram svg');
            if (svg) {{
                const canvas = document.createElement('canvas');
                const ctx = canvas.getContext('2d');
                const img = new Image();
                const svgData = new XMLSerializer().serializeToString(svg);
                const blob = new Blob([svgData], {{type: 'image/svg+xml'}});
                const url = URL.createObjectURL(blob);
                
                img.onload = function() {{
                    canvas.width = img.width;
                    canvas.height = img.height;
                    ctx.fillStyle = 'white';
                    ctx.fillRect(0, 0, canvas.width, canvas.height);
                    ctx.drawImage(img, 0, 0);
                    
                    canvas.toBlob(function(blob) {{
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = '{diagram_type}_diagram.png';
                        a.click();
                    }});
                }};
                img.src = url;
            }}
        }}
    </script>
</body>
</html>"""

    def generate_all_diagrams(self, output_dir: str = "."):
        """Generate all Mermaid diagrams as HTML files."""
        diagrams = {
            "table_relationships": {
                "content": self.create_table_relationship_diagram(),
                "title": "SQL Table Relationships (ERD)"
            },
            "daily_base_flow": {
                "content": self.create_daily_base_flow(),
                "title": "Daily Base Process Flow"
            },
            "incremental_flow": {
                "content": self.create_incremental_flow(),
                "title": "Incremental Update Process"
            },
            "performance_comparison": {
                "content": self.create_performance_comparison(),
                "title": "Performance Comparison"
            },
            "data_lineage": {
                "content": self.create_data_lineage(),
                "title": "Data Lineage & Architecture"
            }
        }
        
        for diagram_name, diagram_info in diagrams.items():
            html_content = self.create_html_wrapper(
                diagram_name,
                diagram_info["content"],
                diagram_info["title"]
            )
            
            filename = os.path.join(output_dir, f"mermaid_{diagram_name}.html")
            with open(filename, 'w') as f:
                f.write(html_content)
            
            print(f"Generated: {filename}")

def main():
    """Generate all Mermaid diagrams."""
    visualizer = MermaidSQLVisualizer()
    visualizer.generate_all_diagrams()
    
    print("\n🎉 Generated interactive Mermaid diagrams!")
    print("Open the HTML files in your browser to explore the visualizations.")

if __name__ == '__main__':
    main()