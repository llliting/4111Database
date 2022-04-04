from flask import Flask, Response, request
from flask_cors import CORS
import json
from datetime import datetime
from resources.imdb_artists import IMDB_Artist
from resources.orders_resources import Orders

import rest_utils

app = Flask(__name__)
CORS(app)

service_factory = dict()

##################################################################################################################

# DFF TODO A real service would have more robust health check methods.
# This path simply echoes to check that the app is working.
# The path is /health and the only method is GETs
@app.route("/health", methods=["GET"])
def health_check():
    rsp_data = {"status": "healthy", "time": str(datetime.now())}
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="application/json")
    return rsp


# TODO Remove later. Solely for explanatory purposes.
# The method take any REST request, and produces a response indicating what
# the parameters, headers, etc. are. This is simply for education purposes.
#
@app.route("/api/demo/<parameter1>", methods=["GET", "POST", "PUT", "DELETE"])
@app.route("/api/demo/", methods=["GET", "POST", "PUT", "DELETE"])
def demo(parameter1=None):
    """
    Returns a JSON object containing a description of the received request.

    :param parameter1: The first path parameter.
    :return: JSON document containing information about the request.
    """

    # DFF TODO -- We should wrap with an exception pattern.
    #

    # Mostly for isolation. The rest of the method is isolated from the specifics of Flask.
    inputs = rest_utils.RESTContext(request, {"parameter1": parameter1})

    # DFF TODO -- We should replace with logging.
    r_json = inputs.to_json()
    msg = {
        "/demo received the following inputs": inputs.to_json()
    }
    print("/api/demo/<parameter> received/returned:\n", msg)

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp

##################################################################################################################


@app.route('/')
def hello_world():
    return '<u>Hello World!</u>'

@app.route('/api/<resource_collection>', methods=['GET', 'POST'])
#@app.route("/api/<resource_collection>/<parameter1>", methods=["GET", "POST"])

def do_resource_collection(resource_collection):
    """
    1. HTTP GET return all resources.
    2. HTTP POST with body --> create a resource, i.e --> database.
    :return:
    """
    request_inputs = rest_utils.RESTContext(request, resource_collection)
    svc = service_factory.get(resource_collection, None)

    if request_inputs.method == "GET":
        res = svc.get_by_template(path=None,
                                  template=request_inputs.args,
                                    field_list=request_inputs.fields,
                                    limit=request_inputs.limit,
                                    offset=request_inputs.offset)

        res = request_inputs.add_pagination(res)
        if res is None:
            rsp = Response("ERROR", status=404, content_type="text/plain")
        else:
            rsp = Response(json.dumps(res, default=str), status=200, content_type="application/json")

    elif request_inputs.method == "POST":
        data = request_inputs.data
        print(data)
        res = svc.create(data)
        if res is None:
            rsp = Response("ERROR", status=404, content_type="text/plain")
        else:
            rsp = Response("CREATED", status=201, content_type="text/plain")
    else:
        rsp = Response("NOT IMPLEMENTED", status=501, content_type="text/plain")

    return rsp


@app.route('/api/<resource_collection>/<resource_id>', methods=['GET', 'PUT', 'DELETE'])
def specific_resource(resource_collection, resource_id):
    """
    1. Get a specific one by ID.
    2. Update body and update.
    3. Delete would ID and delete it.
    :param user_id:
    :return:
    """
    request_inputs = rest_utils.RESTContext(request, resource_collection)
    svc = service_factory.get(resource_collection)

    if request_inputs.method == "GET":
        res = svc.get_resource_by_id(resource_id)
        if res is None:
            rsp = Response("ERROR", status=404, content_type="text/plain")
        else:
            rsp = Response(json.dumps(res, default=str), status=200, content_type="application/json")

    elif request_inputs.method == "PUT":
        res = svc.update_resource_by_id(resource_id, request_inputs.data)
        if res == 1:
            rsp = Response(json.dumps(res, default=str), status=200, content_type="application/json")
        else:
            rsp = Response(json.dumps(res, default=str), status=404, content_type="application/json")

    elif request_inputs.method == "DELETE":
        res = svc.delete_resource_by_id(resource_id)
        if res == 1:
            rsp = Response(json.dumps(res, default=str), status=200, content_type="application/json")
        else:
            rsp = Response(json.dumps(res, default=str), status=404, content_type="application/json")


    else:
        rsp = Response("NOT IMPLEMENTED", status=501, content_type="text/plain")
a    return rsp


if __name__ == '__main__':
    service_factory['imdb_artists'] = IMDB_Artist()
    service_factory['orders_resources'] = Orders()
    app.run(host="0.0.0.0", port=5003)
