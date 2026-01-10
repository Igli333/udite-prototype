from flask import Blueprint, request, jsonify
from .. import service

event_classifier = Blueprint('event_classifier', __name__)
sync_service = service.DataSyncService()

@event_classifier.route('', methods=['GET'])
def pff():
    pass