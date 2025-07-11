# Container Planning Station Comparison

This document compares the container planning data between two delivery stations:

1. **DAU1** (June 3, 2025)
2. **DJT6** (June 2, 2025)

## Data Overview

| Metric | DAU1 (June 3) | DJT6 (June 2) |
|--------|--------------|--------------|
| Total Packages Analyzed | 42 | 109 |
| Unique Originating Nodes | 6 | 13 |
| Planned Packages | 42 (100%) | 67 (61%) |
| Inducted Packages | 40 (95%) | 103 (94%) |
| Inducted as Planned | 40 (95%) | 61 (56%) |
| SLAM Post DCAP | 0 (0%) | 42+ (39%+) |

## Key Differences

### Planning Effectiveness

1. **DAU1 (June 3)**: 
   - All packages (100%) were planned
   - 95% were inducted as planned
   - Perfect planning-to-execution alignment

2. **DJT6 (June 2)**:
   - Only 61% of packages were planned
   - 56% were inducted as planned
   - 39% were inducted without prior planning (SLAM Post DCAP)
   - More variability in planning effectiveness

### Originating Nodes

1. **DAU1 (June 3)**:
   - 6 unique originating nodes
   - Primarily from SAT2 (FC), AUS2 (FC), HOU5 (SC)

2. **DJT6 (June 2)**:
   - 13 unique originating nodes (more diverse supply chain)
   - Includes PSP1, SAN3, SBD6, OXR1, and others
   - More complex upstream network

### Package Processing

1. **DAU1 (June 3)**:
   - Consistent condition status ("On Time for induct-IaP")
   - Consistent timing patterns
   - Lower variance in processing times

2. **DJT6 (June 2)**:
   - Mixed condition statuses, including "SLAM Post DCAP-InP"
   - More variability in timing and processing
   - Higher variance in stop numbers and route assignments

## Implications

1. **Planning Efficiency**:
   - DAU1 shows superior planning efficiency with 100% of packages being planned
   - DJT6 has significant unplanned volume (39%), suggesting operational challenges

2. **Network Complexity**:
   - DJT6 has more complex upstream supply chain with twice as many originating nodes
   - This likely contributes to the increased planning challenges

3. **Process Stability**:
   - DAU1 shows more stable and predictable operations
   - DJT6 shows higher variance in timing and execution

4. **Volume**:
   - DJT6 processed significantly more packages (109 vs 42 in the sample)
   - Higher volume may contribute to increased planning challenges

## Recommendations

1. **Cross-station Learning**:
   - Investigate DAU1's planning processes to identify best practices
   - Apply these practices to improve DJT6's planning effectiveness

2. **Supply Chain Alignment**:
   - Analyze the correlation between originating nodes and planning effectiveness
   - Work with upstream nodes to improve predictability for DJT6

3. **Capacity Planning**:
   - Evaluate if DJT6's planning challenges relate to volume or capacity constraints
   - Adjust staffing or equipment as needed

4. **Data-driven Decision Making**:
   - Use these metrics as KPIs to track improvement over time
   - Set targets for reducing unplanned packages