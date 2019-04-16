import ciso8601
import json

class MetadataQueries:
    def __init__(self):
        pass

    #############
    # Query Name:
    #   query1_totalRecordCount
    # Pseudoquery:
    #   totalRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < 4/12/2019
    def query1_totalRecordCount(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        received_at_string = json.loads(record)['metadata']['odeReceivedAt']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time

    #############
    # Query Name:
    #   query2_timBroadcastRecordCount
    # Pseudoquery:
    #   timBroadcastRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < 4/12/2019 AND metadata.recordGeneratedBy == TMC
    def query2_timBroadcastRecordCount(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        record_generated_by = record_object['metadata']['recordGeneratedBy']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time and record_generated_by == 'TMC'

    #############
    # Query Name:
    #   query3_goodOtherRecordCount
    # Pseudoquery:
    #   goodOtherRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 2/13/2019 AND metadata.odeReceivedAt < dateOfBugFix AND metadata.payloadType != us.dot.its.jpo.ode.model.OdeBsmPayload AND metadata.recordGeneratedBy != TMC
    def query3_goodOtherRecordCount(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2019-02-13T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        payload_type = record_object['metadata']['payloadType']
        record_generated_by = record_object['metadata']['recordGeneratedBy']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time and payload_type != 'us.dot.its.jpo.ode.model.OdeBsmPayload' and record_generated_by != 'TMC'

    #############
    # Query Name:
    #   query4_badBsmRecordCount
    # Pseudoquery:
    #   badBsmRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < dateOfBugFix AND metadata.payloadType == us.dot.its.jpo.ode.model.OdeBsmPayload
    def query4_badBsmRecordCount(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        payload_type = record_object['metadata']['payloadType']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time and payload_type == 'us.dot.its.jpo.ode.model.OdeBsmPayload'

    #############
    # Query Name:
    #   query5_badOtherRecordCount
    # Pseudoquery:
    #   badOtherRecordCount = SELECT COUNT(*) WHERE metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt <= 2/12/2019 AND metadata.recordGeneratedBy != TMC AND metadata.payloadType != us.dot.its.jpo.ode.model.OdeBsmPayload
    def query5_badOtherRecordCount(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time = ciso8601.parse_datetime_as_naive('2019-02-13T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        record_generated_by = record_object['metadata']['recordGeneratedBy']
        payload_type = record_object['metadata']['payloadType']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return received_at > start_time and received_at < end_time and record_generated_by != 'TMC' and payload_type != 'us.dot.its.jpo.ode.model.OdeBsmPayload'

######

    #############
    # Query Name:
    #   query8_earliestGeneratedAt
    # Pseudoquery:
    #   earliestGeneratedAt = SELECT MIN(metadata.recordGeneratedAt)
    # WHERE (metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt <= 2/12/2019 AND metadata.recordGeneratedBy != TMC AND metadata.payloadType != us.dot.its.jpo.ode.model.OdeBsmPayload)
    # OR (metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < dateOfBugFix AND metadata.payloadType == us.dot.its.jpo.ode.model.OdeBsmPayload)
    def query8_earliestGeneratedAt(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time_nontmc = ciso8601.parse_datetime_as_naive('2019-02-13T00:00:00.000Z')
        end_time_full = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        record_generated_by = record_object['metadata']['recordGeneratedBy']
        record_generated_at_string = record_object['metadata']['recordGeneratedAt']
        payload_type = record_object['metadata']['payloadType']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
            record_generated_at = ciso8601.parse_datetime_as_naive(record_generated_at_string)
            if not hasattr(self, 'earliest_generated_at'):
                self.earliest_generated_at = record_generated_at
                print("Found first record_generated_at: %s" % self.earliest_generated_at)
            else:
                if received_at > start_time and received_at < end_time_nontmc and record_generated_by != 'TMC' and payload_type != 'us.dot.its.jpo.ode.model.OdeBsmPayload':
                    if record_generated_at < self.earliest_generated_at:
                        self.earliest_generated_at = record_generated_at
                        print("Found new earliest_generated_at: %s" % self.earliest_generated_at)
                elif received_at > start_time and received_at < end_time_full and payload_type == 'us.dot.its.jpo.ode.model.OdeBsmPayload':
                    if record_generated_at < self.earliest_generated_at:
                        self.earliest_generated_at = record_generated_at
                        print("Found new earliest_generated_at: %s" % self.earliest_generated_at)
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return True

    #############
    # Query Name:
    #   query9_latestGeneratedAt
    # Pseudoquery:
    #   latestGeneratedAt = SELECT MAX(metadata.recordGeneratedAt)
    # WHERE (metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt <= 2/12/2019 AND metadata.recordGeneratedBy != TMC AND metadata.payloadType != us.dot.its.jpo.ode.model.OdeBsmPayload)
    # OR (metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < dateOfBugFix AND metadata.payloadType == us.dot.its.jpo.ode.model.OdeBsmPayload)
    def query9_latestGeneratedAt(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time_nontmc = ciso8601.parse_datetime_as_naive('2019-02-13T00:00:00.000Z')
        end_time_full = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        record_generated_by = record_object['metadata']['recordGeneratedBy']
        record_generated_at_string = record_object['metadata']['recordGeneratedAt']
        payload_type = record_object['metadata']['payloadType']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
            record_generated_at = ciso8601.parse_datetime_as_naive(record_generated_at_string)
            if not hasattr(self, 'latest_generated_at'):
                self.latest_generated_at = record_generated_at
                print("Found last record_generated_at: %s" % self.latest_generated_at)
            else:
                if received_at > start_time and received_at < end_time_nontmc and record_generated_by != 'TMC' and payload_type != 'us.dot.its.jpo.ode.model.OdeBsmPayload':
                    if record_generated_at > self.latest_generated_at:
                        self.latest_generated_at = record_generated_at
                        print("Found new latest_generated_at: %s" % self.latest_generated_at)
                elif received_at > start_time and received_at < end_time_full and payload_type == 'us.dot.its.jpo.ode.model.OdeBsmPayload':
                    if record_generated_at > self.latest_generated_at:
                        self.latest_generated_at = record_generated_at
                        print("Found new latest_generated_at: %s" % self.latest_generated_at)
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return True

    #############
    # Query Name:
    #   query11_invalidS3FileCount
    # Pseudoquery:
    #   invalidS3FileCount = SELECT COUNT(s3-filename)
    # WHERE (metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt <= 2/12/2019 AND metadata.recordGeneratedBy != TMC AND metadata.payloadType != us.dot.its.jpo.ode.model.OdeBsmPayload)
    # OR (metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < dateOfBugFix AND metadata.payloadType == us.dot.its.jpo.ode.model.OdeBsmPayload)
    def query11_invalidS3FileCount(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time_nontmc = ciso8601.parse_datetime_as_naive('2019-02-13T00:00:00.000Z')
        end_time_full = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        record_generated_by = record_object['metadata']['recordGeneratedBy']
        record_generated_at_string = record_object['metadata']['recordGeneratedAt']
        payload_type = record_object['metadata']['payloadType']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
            record_generated_at = ciso8601.parse_datetime_as_naive(record_generated_at_string)
            if received_at > start_time and received_at < end_time_nontmc and record_generated_by != 'TMC' and payload_type != 'us.dot.its.jpo.ode.model.OdeBsmPayload':
                return True
            elif received_at > start_time and received_at < end_time_full and payload_type == 'us.dot.its.jpo.ode.model.OdeBsmPayload':
                return True
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return False

    #############
    # Query Name:
    #   query13_listOfLogFilesBefore
    # Pseudoquery:
    #   listOfLogFilesBefore = SELECT metadata.logFileName
    # WHERE (metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt <= 2/12/2019 AND metadata.recordGeneratedBy != TMC AND metadata.payloadType != us.dot.its.jpo.ode.model.OdeBsmPayload)
    # OR (metadata.odeReceivedAt >= 12/3/2018 AND metadata.odeReceivedAt < dateOfBugFix AND metadata.payloadType == us.dot.its.jpo.ode.model.OdeBsmPayload)
    def query13_listOfLogFilesBefore(self, record):
        start_time = ciso8601.parse_datetime_as_naive('2018-12-03T00:00:00.000Z')
        end_time_nontmc = ciso8601.parse_datetime_as_naive('2019-02-13T00:00:00.000Z')
        end_time_full = ciso8601.parse_datetime_as_naive('2019-04-12T00:00:00.000Z')
        record_object = json.loads(record)
        received_at_string = record_object['metadata']['odeReceivedAt']
        record_generated_by = record_object['metadata']['recordGeneratedBy']
        record_generated_at_string = record_object['metadata']['recordGeneratedAt']
        payload_type = record_object['metadata']['payloadType']
        try:
            received_at = ciso8601.parse_datetime_as_naive(received_at_string)
            record_generated_at = ciso8601.parse_datetime_as_naive(record_generated_at_string)
            if (received_at > start_time and received_at < end_time_nontmc and record_generated_by != 'TMC' and payload_type != 'us.dot.its.jpo.ode.model.OdeBsmPayload') or (received_at > start_time and received_at < end_time_full and payload_type == 'us.dot.its.jpo.ode.model.OdeBsmPayload'):
                if not hasattr(self, 'log_file_list'):
                    self.log_file_list = {}
                if record_object['metadata'].get('logFileName') is None:
                    if '_missing' not in self.log_file_list:
                        self.log_file_list['_missing'] = 1
                    else:
                        self.log_file_list['_missing'] = self.log_file_list['_missing'] + 1
                else:
                    if record_object['metadata']['logFileName'] not in self.log_file_list:
                        self.log_file_list[record_object['metadata']['logFileName']] = 1
                    else:
                        self.log_file_list[record_object['metadata']['logFileName']] = self.log_file_list[record_object['metadata']['logFileName']] + 1
                return True
        except Exception as e:
            print("[ERROR] Was unable to parse timestamp. Timestamp: %s. Error: %s" % (received_at_string, str(e)))
            raise SystemExit
        return False
