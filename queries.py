import ciso8601
import json

class MetadataQueries:
    #############
    # Query Name:
    #   query1_totalRecordCount
    # Pseudoquery:
    #   totalRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < 4/12/2019
    def query1_totalRecordCount(record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        received_at_string = json.loads(record)['metadata']['odeReceivedAt']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse 'odeReceivedAt'. odeReceivedAt: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time

    #############
    # Query Name:
    #   query2_timBroadcastRecordCount
    # Pseudoquery:
    #   timBroadcastRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < 4/12/2019 AND metadata.recordGeneratedBy == TMC
    def query2_timBroadcastRecordCount(record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        record_generated_by = record_object['metadata']['recordGeneratedBy']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse 'odeReceivedAt'. odeReceivedAt: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time and record_generated_by == 'TMC'

    #############
    # Query Name:
    #   query3_goodOtherRecordCount
    # Pseudoquery:
    #   goodOtherRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 2/13/2019 AND metadata.odeReceivedAt < dateOfBugFix AND metadata.payloadType != us.dot.its.jpo.ode.model.OdeBsmPayload
    def query3_goodOtherRecordCount(record):
        start_time = ciso8601.parse_datetime_as_naive('2019-02-13T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        payload_type = record_object['metadata']['payloadType']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse 'odeReceivedAt'. odeReceivedAt: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time and payload_type == 'us.dot.its.jpo.ode.model.OdeBsmPayload'

    #############
    # Query Name:
    #   query4_badBsmRecordCount
    # Pseudoquery:
    #   badBsmRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < dateOfBugFix AND metadata.payloadType == us.dot.its.jpo.ode.model.OdeBsmPayload
    def query4_badBsmRecordCount(record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        payload_type = record_object['metadata']['payloadType']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse 'odeReceivedAt'. odeReceivedAt: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time and payload_type == 'us.dot.its.jpo.ode.model.OdeBsmPayload'
