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
    _success = ""
    _name = "RequestData"
    _response_code = ""

    def __init__(self,id, duration, success, status_code):
        super().__init__(id)
        self._duration = duration
        self._success = success
        self._response_code = status_code
    
    def to_json(self):
        return {
            "id": self._id,
            "duration": self._duration,
            "success": self._success,
            "name": self._name,
            "responseCode": self._response_code
        }

class RemoteDependencyData(Domain):

    _duration = ""
    _success = ""
    _name = "RemoteDependencyData"
    _result_code = ""

    def __init__(self,id, duration, success, status_code):
        super().__init__(id)
        self._duration = duration
        self._success = success
        self._result_code = status_code

    def to_json(self):
        return {
            "id": self._id,
            "duration": self._duration,
            "success": self._success,
            "name": self._name,
            "resutltCode": self._result_code 
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

        # TODO, length check
        top_span = span_datas[0]
        trace_id = top_span.context.trace_id if top_span.context is not None \
        else ""

        #self.base_req_json["tags"]["ai.operation.id"] = trace_id

        is_request = top_span.attributes.get("/http/method","")
        if (is_request):
            # Request Data
            self._envelope.SetEnvelopeName("RequestData")
            lis = self.convertToAppInsightFormat(span_datas)
        else:
            lis = self.convertToAppInsightFormat(span_datas,"RemoteDependencyData")
        
        for item in lis:
                self.sendData(item)

    def convertToAppInsightFormat(self,span_datas,bond_type="RequestData"):
        converted_jsons = []
        for span_data in span_datas:
            req = copy.deepcopy(self._envelope)

            # set envelope time
            req.SetEnvelopeTime(span_data.start_time)

            # data values
            _id = span_data.span_id

            trace_id = span_data.context.trace_id if span_data.context is not None \
            else ""
            parent_id = span_data.context.parent_span_id if span_data.context is not None \
            else ""

            req.SetEnvelopeTags(str(parent_id), str(trace_id))

            if bond_type == "RequestData" and span_data.status is not None:
                data = RequestData()
                data['responseCode'] = span_data.attributes.get('/http/status_code', "")
            else:
                data = RemoteDependencyData()
                data['resultCode'] = span_data.attributes.get('requests/status_code', "")

            data['id'] = span_data.span_id

            # TODO: fix this
            st_dt_obj = datetime.strptime(span_data.start_time,"%Y-%m-%dT%H:%M:%S.%fZ")
            end_dt_obj = datetime.strptime(span_data.end_time,"%Y-%m-%dT%H:%M:%S.%fz")
            diff = end_dt_obj - st_dt_obj
            duration_str = str(int(diff.total_seconds() * 1000))[:6]

            data['duration'] = duration_str # TODO
            data['name'] = span_data.name
            converted_jsons.insert({"request":req,"context:":{}})
        return converted_jsons

    def transform(self,span_data):
        """
        Convert span_data to request json
        """
        

    def sendData(self,request):
        """
        :type request: dictionary
        :param request: Transformed Dictionary
        {
            'request':{

            }
            'context':{

            }
        }
        """
        app_insight_req = request.get('request')
        app_insight_ctx = request.get('context')
        self.sendToEndpoint(app_insight_req)
        #self.sendToEndpoint(app_insight_ctx)

    def sendToEndpoint(self,data):
        encoded_data = json.dumps(data).encode('utf-8')
        r = self.http.request('POST',
            self.endpoint,
            body=encoded_data,
            headers={'Content-Type': 'application/json'}
        )

    def export(self, span_datas):
        """
        :type span_datas: list of :class:
            `~opencensus.trace.span_data.SpanData`
        :param list of opencensus.trace.span_data.SpanData span_datas:
            SpanData tuples to export
        """
        self.transport.export(span_datas)
