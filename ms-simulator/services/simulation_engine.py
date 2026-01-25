import json
import os

from domain.models import SimulationRequest, SimulationResult, Intervention, TriggerType, SensorReading
from services.data_connector import DataConnector
from datetime import datetime


class SimulationEngine:
    def __init__(self):
        self.data_connector = DataConnector()
        self.results_dir = "ms-simulator/mock_simulation_results"
        os.makedirs(self.results_dir, exist_ok=True)

    def run_simulation(self, request: SimulationRequest) -> SimulationResult:
        """
        Executes a simulation based on the aligned triggers and sensor readings.
        """

        # 1. Gather Context
        ctx_node = {}
        if request.target_id:
            ctx_node = self.data_connector.get_node_details(request.target_id)

        # Get mocked sensor readings mimicking Data Sync
        readings = self.data_connector.get_district_readings("district_1")

        # 2. Simulation Logic
        severity = 0.0
        impact = "Minimal impact detected."
        interventions = []
        affected = []

        # --- EVENT CLASSIFIER / DATA SYNC ALIGNED LOGIC ---

        if request.trigger_type == TriggerType.GAS_LEAK_DETECTED:
            severity = 0.95
            impact = "CRITICAL: Gas leak detected. Explosion risk."
            affected = ["Sector_residential_4", "School_District_2"]
            interventions.append(
                Intervention(id="INT_GAS_01", description="Trigger Auto-Shutoff Valves",
                             estimated_impact="Stop flow immediately")
            )
            interventions.append(
                Intervention(id="INT_GAS_02", description="Evacuate 500m Radius via SMS Alert",
                             estimated_impact="Ensure resident safety")
            )

        elif request.trigger_type == TriggerType.FLOOD_RISK:
            severity = 0.85
            impact = "Rising water levels threaten low-lying infrastructure."
            affected = ["River_Bank_Roads", "Subway_Stations_North"]
            interventions.append(
                Intervention(id="INT_FLD_01", description="Deploy Mobile Barriers",
                             estimated_impact="Prevent station flooding")
            )

        elif request.trigger_type == TriggerType.CELL_TOWER_DOWN:
            severity = 0.7
            impact = "Connectivity lost in Sector 3. Emergency comms at risk."
            affected = ["Sector_3", "Business_Park"]
            interventions.append(
                Intervention(id="INT_TEL_01", description="Deploy COW (Cell on Wheels)",
                             estimated_impact="Restore 60% coverage")
            )

        elif request.trigger_type == TriggerType.TRAFFIC_GRIDLOCK:
            severity = 0.65
            impact = "Standstill traffic impacting emergency response routes."
            affected = ["Main_Highway_South", "Bridge_Access"]
            interventions.append(
                Intervention(id="INT_TRF_01", description="Reroute signals to flush Highway South",
                             estimated_impact="Clearing time 20min")
            )

        elif request.trigger_type == TriggerType.HOSPITAL_OVERLOAD:
            severity = 0.9
            impact = "ER Capacity exceeded. Patient intake stalled."
            affected = ["City_General_Hospital"]
            interventions.append(
                Intervention(id="INT_HOS_01", description="Redirect ambulances to District 2 Clinics",
                             estimated_impact="Load distribution balanced")
            )

        elif request.trigger_type == TriggerType.WILDFIRE_RISK_HIGH:
            severity = 0.8
            impact = "Dry conditions and wind favor rapid fire spread."
            affected = ["National_Park_Border", "Suburbs_East"]
            interventions.append(
                Intervention(id="INT_FIR_01", description="Deploy Drones for Thermal Monitoring",
                             estimated_impact="Early detection")
            )

        # --- LEGACY / FALLBACK ---
        elif request.trigger_type == TriggerType.NODE_FAILURE:
            severity = 0.5
            impact = "Infrastructure node offline."
            affected = ["Local_Access"]
            interventions.append(Intervention(id="INT_GEN_01", description="Dispatch Technician",
                                              estimated_impact="Restore service in 2h"))

        else:
            impact = "Event type recognized but no specific simulation rule found."
            severity = 0.1

        # 3. Construct Result
        result = SimulationResult(
            impact_assessment=impact,
            severity_score=severity,
            affected_areas=affected,
            interventions=interventions,
            simulated_metrics={
                "readings_analyzed": len(readings),
                "district_status": "MONITORED",
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
