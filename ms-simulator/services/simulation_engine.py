from domain.models import SimulationRequest, SimulationResult, Intervention
from services.data_connector import DataConnector
import json
import os
from datetime import datetime

class SimulationEngine:
    def __init__(self):
        self.data_connector = DataConnector()
        self.results_dir = "ms-simulator/mock_simulation_results"
        os.makedirs(self.results_dir, exist_ok=True)

    def run_simulation(self, request: SimulationRequest) -> SimulationResult:
        """
        Executes a simulation based on the request and current context.
        """

        
        # 1. Gather Context
        ctx_node = {}
        if request.target_id:
            ctx_node = self.data_connector.get_node_details(request.target_id)
        
        # Assume a default district for this prototype
        ctx_district = self.data_connector.get_district_status("district_1")

        # 2. Simulation Logic (Simple Rule-Based)
        severity = 0.0
        impact = "Minimal impact detected."
        interventions = []
        affected = []

        if request.trigger_type == "NODE_FAILURE":
            if ctx_node.get("node_type") == "TRAFFIC_LIGHT":
                severity = 0.8
                impact = "High traffic congestion expected due to signal failure."
                affected = ["Road_A", "Road_B", "Intersection_5"]
                interventions.append(
                    Intervention(id="INT_001", description="Deploy Traffic Police to Intersection_5", estimated_impact="Reduce delay by 40%")
                )
                interventions.append(
                   Intervention(id="INT_002", description="Reroute Public Transport Line 4", estimated_impact="Avoid deadlock") 
                )
            else:
                 severity = 0.4
                 impact = "Minor service degradation."
                 affected = ["Local_Area"]

        elif request.trigger_type == "CRITICAL_METRIC":
             # Example: Flood level high
             severity = 0.9
             impact = "Critical infrastructure at risk."
             affected = ["Sector_7", "Sector_8"]
             interventions.append(
                 Intervention(id="INT_003", description="Activate Flood Barriers", estimated_impact="Prevent flooding of Sector 7")
             )

        elif request.trigger_type == "TRAFFIC_JAM":
            severity = 0.6
            impact = "Severe congestion detected on main artery."
            affected = ["Highway_1", "Downtown_Access"]
            interventions.append(
                Intervention(id="INT_004", description="Adjust Traffic Light Sync", estimated_impact="Improve flow by 15%")
            )
            interventions.append(
                Intervention(id="INT_005", description="Open Emergency Lane", estimated_impact="Relieve congestion by 10%")
            )

        elif request.trigger_type == "ENVIRONMENTAL_ALERT":
            # example: Air Quality Index dangerous
            severity = 0.75
            impact = "Hazardous air quality levels detected."
            affected = ["City_Center", "Parks"]
            interventions.append(
                Intervention(id="INT_006", description="Issue Public Health Warning", estimated_impact="Reduce outdoor activity")
            )
            interventions.append(
                 Intervention(id="INT_007", description="Restrict Heavy Vehicle Access", estimated_impact="Reduce emission by 20%")
            )

        # 3. Construct Result
        result = SimulationResult(
            impact_assessment=impact,
            severity_score=severity,
            affected_areas=affected,
            interventions=interventions,
            simulated_metrics={
                "projected_wait_time": "25m" if severity > 0.7 else "5m",
                "risk_level": "HIGH" if severity > 0.7 else "LOW"
            }
        )

        # 4. Dump to JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sim_{timestamp}_{request.trigger_type}.json"
        filepath = os.path.join(self.results_dir, filename)
        
        with open(filepath, "w") as f:
            f.write(result.model_dump_json(indent=2))
            
        # print(f"Simulation result dumped to {filepath}")

        return result
