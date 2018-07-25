# Copyright 2017, OpenCensus Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Export the trace spans to a local file."""

import json

from opencensus.trace import span_data
from opencensus.trace.exporters import base
from opencensus.trace.exporters.transports import sync
from datetime import datetime
import urllib3
import copy

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
DEFAULT_ENDPOINT = 'https://dc.services.visualstudio.com/v2/track'

class Envelope(object):
    _ikey = ""
    _time = ""
    _name = ""
    _tags = None
    _data = None

    def __init__(self,ikey):
        self._ikey = ikey
    
    def SetEnvelopeName(self,name):
        self._name = name
    
    def SetEnvelopeTime(self,time):
        self._time = time
    
    def SetEnvelopeTags(self,parentId,traceId):
        self._tags = EnvelopeTags()
        self._tags.SetTagsParentid(parentId)
        self._tags.SetTagsTraceId(traceId)

    def SetEnvelopeData(self,data):
        self._data = data
    
    def toJson(self):
        return {
            "iKey": self._ikey,
            "time": self._time,
            "name": self._name,
            "tags": self._tags.toJson(),
            "data": self._data.toJson()
        }

class EnvelopeTags(object):
    _parentId = ""
    _traceId = ""

    def SetTagsParentid(self,parentId):
        self._parentId = parentId

    def SetTagsTraceId(self, traceId):
        self._traceId = traceId

    def toJson(self):
        return {
            "ai.operation.id": self._traceId,
            "ai.operation.parentId": self._parentId
        }

class Data(object):
    _baseType = ""
    _baseData = None

class Domain(object):
    _id = ""

    def __init__(self,id):
        self._id = id

class RequestData(Domain):

    _duration = ""
    _success = False
    _name = "RequestData"
    _response_code = ""

    def __init__(self,id, duration, status_code):
        super().__init__(id)
        self._duration = duration
        self._response_code = status_code
        self._success = int(status_code)<400 if status_code is not "" \
        else "False"

    def toJson(self):
        return {
            "baseType": self._name,
            "baseData": {
                "id": self._id,
                "duration": self._duration,
                "success": self._success,
                "name": self._name,
                "responseCode": self._response_code
            }
        }

class RemoteDependencyData(Domain):

    _duration = ""
    _success = False
    _name = "RemoteDependencyData"
    _result_code = ""
    _data = ""
    _target = ""
    _type = ""

    def __init__(self,id, duration, status_code, data, type):
        super().__init__(id)
        self._duration = duration
        self._data = data
        self._target = data
        self._type = type
        self._result_code = status_code
        self._success = int(status_code) < 400 if status_code is not "" \
        else "False"

    def toJson(self):
        return {
            "baseType": self._name,
            "baseData":{
                "id": self._id,
                "duration": self._duration,
                "success": self._success,
                "name": self._name,
                "resultCode": self._result_code,
                "data": self._data,
                "target": self._target,
                "type": self._type
            } 
        }

class AppInsightExporter(base.Exporter):
    """
    :type instrumentation_key: str
    :param instrumentation_key: The unique key required to push
        data to your app insight portal 

    :type transport: :class:`type`
    :param transport: Class for creating new transport objects. It should
                      extend from the base :class:`.Transport` type and
                      implement :meth:`.Transport.export`. Defaults to
                      :class:`.SyncTransport`. The other option is
                      :class:`.BackgroundThreadTransport`.

    :type endpoint: str
    :param endpoint: the endpoint where the data is pushed to

    """

    def __init__(self, instrumentation_key,
                 transport=sync.SyncTransport,
                 endpoint=DEFAULT_ENDPOINT):
        self.instrumentation_key = instrumentation_key
        self.transport = transport(self)
        self.endpoint = endpoint

        self.http = urllib3.PoolManager()
        self._envelope = None

    def emit(self, span_datas):
        """
        :type span_datas: list of :class:
            `~opencensus.trace.span_data.SpanData`
        :param list of opencensus.trace.span_data.SpanData span_datas:
            SpanData tuples to emit
        """
        self._envelope = Envelope(self.instrumentation_key)
        lis = self.convertToAppInsightFormat(span_datas)
        
        for item in lis:
            self.sendToEndpoint(item)

    def convertToAppInsightFormat(self,span_datas):
        converted_jsons = []
        i = 0
        for span_data in span_datas:

            cur_req = copy.deepcopy(self._envelope)
            # time
            cur_req.SetEnvelopeTime(span_data.start_time)

            # tags
            trace_id = span_data.context.trace_id if span_data.context is not None \
            else ""
            parent_id = span_data.parent_span_id
            cur_req.SetEnvelopeTags(str(parent_id), str(trace_id))

            # data values
            _id = span_data.span_id
            _duration = self.getDuration(span_data)

            _type = self.getType(span_data)
            if (_type == "RequestData"):
                data = RequestData(span_data.span_id,
                    _duration,
                    self.getStatusCode(span_data,_type)
                )
                cur_req.SetEnvelopeName(_type)
                cur_req.SetEnvelopeData(data)
            else:
                data = RemoteDependencyData(span_data.span_id,
                    _duration,
                    self.getStatusCode(span_data,_type),
                    self.getTargetData(span_data),
                    self.getDependencyType(span_data)
                )
                cur_req.SetEnvelopeName(_type)
                cur_req.SetEnvelopeData(data)
            
            converted_jsons.append( cur_req.toJson())
            i += 1

        return converted_jsons
        
    def sendToEndpoint(self,data):

        encoded_data = json.dumps(data)
        r = self.http.request('POST',
            self.endpoint,
            body=encoded_data,
            headers={'Content-Type': 'application/json'}
        )

    def getType(self,span_data):
        if span_data.attributes.get("/http/method"):
            return "RequestData"
        return "RemoteDependencyData"

    def getStatusCode(self,span_data,bond_type):
        if bond_type == "RequestData" and span_data.attributes is not None:
            return  span_data.attributes.get('/http/status_code', "")
        return span_data.attributes.get('requests/status_code', "")

    def getTargetData(self, span_data):
        if span_data.attributes is not None:
            return span_data.attributes.get('requests/url',"")
        return ""

    def getDependencyType(self, span_data):
        if span_data.name is not None:
            return span_data.name
        return ""

    def getDuration(self,span_data):
        st_dt_obj = datetime.strptime(span_data.start_time,"%Y-%m-%dT%H:%M:%S.%fZ")
        end_dt_obj = datetime.strptime(span_data.end_time,"%Y-%m-%dT%H:%M:%S.%fz")
        diff = end_dt_obj - st_dt_obj
        # sending duration in microseconds because AI expects that
        # AI will convert this to miliseonds before population its database
        return str(int(diff.total_seconds() * 1000000))[:6]

    def export(self, span_datas):
        """
        :type span_datas: list of :class:
            `~opencensus.trace.span_data.SpanData`
        :param list of opencensus.trace.span_data.SpanData span_datas:
            SpanData tuples to export
        """
        self.transport.export(span_datas)