import uuid

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime


class TriggerType(str, Enum):
    # From ms-event-classifier
    TRAFFIC_GRIDLOCK = "TRAFFIC_GRIDLOCK"
    TRAFFIC_CONGESTION = "TRAFFIC_CONGESTION"
    SERVICE_CANCELLATION = "SERVICE_CANCELLATION"
    MAJOR_DELAY = "MAJOR_DELAY"
    RESOURCE_SHORTAGE = "RESOURCE_SHORTAGE"
    HOSPITAL_OVERLOAD = "HOSPITAL_OVERLOAD"
    FLOOD_RISK = "FLOOD_RISK"
    HEAVY_RAIN_EVENT = "HEAVY_RAIN_EVENT"
    AIR_QUALITY_ALERT = "AIR_QUALITY_ALERT"
    WILDFIRE_RISK_HIGH = "WILDFIRE_RISK_HIGH"
    LOCAL_OUTAGE = "LOCAL_OUTAGE"
    PIPE_BURST_RISK = "PIPE_BURST_RISK"
    GAS_LEAK_DETECTED = "GAS_LEAK_DETECTED"
    BIN_OVERFLOW = "BIN_OVERFLOW"
    CELL_TOWER_DOWN = "CELL_TOWER_DOWN"

    # Generic/Legacy
    NODE_FAILURE = "NODE_FAILURE"
    CRITICAL_METRIC = "CRITICAL_METRIC"
    MANUAL = "MANUAL"


class SimulationRequest(BaseModel):
    trigger_type: TriggerType
    target_id: Optional[str] = None  # example: Node ID or Metric Name
    parameters: Dict[str, Any] = {}  # Extra context


class Intervention(BaseModel):
    id: str
    description: str
    estimated_impact: str


class Location(BaseModel):
    lat: float
    lng: float


class SensorReading(BaseModel):
    """
    Mirrors ms-data-sync's SensorData model.
    """
    sensor_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    value: float
    unit: str
    location: Location
    system: str
    district: str


class SimulationResult(BaseModel):
    impact_assessment: str
    severity_score: float  # range: 0.0 to 1.0
    affected_areas: List[str]
    interventions: List[Intervention]
    simulated_metrics: Dict[str, Any]  # example: {"traffic_delay": "15m"}
