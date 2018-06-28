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
        self.base_req_json = {
            "iKey": self.instrumentation_key,
            "time": None,
            "name": "RequestData",
            "tags":{
                "ai.operation.id": "",
                "ai.operation.parentId": ""
            },
            "data": {
                "baseType": "RequestData",
                "baseData": {
                    "id": "",
                    "duration": "",
                    "responseCode": "200",
                    "success": "true",
                    "name": "",
                }
            }
        }

    def emit(self, span_datas):
        """
        :type span_datas: list of :class:
            `~opencensus.trace.span_data.SpanData`
        :param list of opencensus.trace.span_data.SpanData span_datas:
            SpanData tuples to emit
        """
        
        top_span = span_datas[0]
        trace_id = top_span.context.trace_id if top_span.context is not None \
        else ""
        self.base_req_json["tags"]["ai.operation.id"] = trace_id
        lis = self.convertToAppInsightFormat(span_datas)
        for item in lis:
            self.sendData(item)

    def convertToAppInsightFormat(self,span_datas):
        lis = [self.transform(span) for span in span_datas]
        return lis

    def transform(self,span_data):
        """
        Convert span_data to request json
        """
        req = copy.deepcopy(self.base_req_json)
        req['time'] = span_data.start_time,
        data = req['data']['baseData']
        data['id'] = span_data.span_id
        

        st_dt_obj = datetime.strptime(span_data.start_time,"%Y-%m-%dT%H:%M:%S.%fZ")
        end_dt_obj = datetime.strptime(span_data.end_time,"%Y-%m-%dT%H:%M:%S.%fz")
        
        diff = end_dt_obj - st_dt_obj
        
        # TODO: fix this
        duration_str = str(int(diff.total_seconds() * 1000))[:6]

        data['duration'] = duration_str # TODO
        data['name'] = span_data.name

        if span_data.status is not None:
            data['responseCode'] = span_data.status.format_status_json()['code']

        if span_data.parent_span_id is not None:
            req["tags"]["ai.operation.parentId"] = str(span_data.parent_span_id)
        
        return {"request":req,"context:":{}}

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
