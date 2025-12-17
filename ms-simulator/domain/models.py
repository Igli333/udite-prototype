from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from enum import Enum

class TriggerType(str, Enum):
    NODE_FAILURE = "NODE_FAILURE"
    CRITICAL_METRIC = "CRITICAL_METRIC"
    MANUAL = "MANUAL"
    TRAFFIC_JAM = "TRAFFIC_JAM"
    ENVIRONMENTAL_ALERT = "ENVIRONMENTAL_ALERT"

class SimulationRequest(BaseModel):
    trigger_type: TriggerType
    target_id: Optional[str] = None # example: Node ID or Metric Name
    parameters: Dict[str, Any] = {} # Extra context

class Intervention(BaseModel):
    id: str
    description: str
    estimated_impact: str

class SimulationResult(BaseModel):
    impact_assessment: str
    severity_score: float #range: 0.0 to 1.0
    affected_areas: List[str]
    interventions: List[Intervention]
    simulated_metrics: Dict[str, Any] # example: {"traffic_delay": "15m"}
