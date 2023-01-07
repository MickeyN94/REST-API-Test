from flask import Flask
from flask_smorest import Api

from resources import blp as ReportBlueprint


def create_app():
    app = Flask(__name__)

    app.config["PROPOGATE_EXCEPTIONS"] = True
    app.config["API_TITLE"] = "Practise REST API"
    app.config["API_VERSION"] = "v1"
    app.config["OPENAPI_VERSION"] = "3.0.3"
    app.config["OPENAPI_URL_PREFIX"] = "/"
    app.config["OPENAPI_SWAGGER_UI_PATH"] = "/swagger-ui"
    app.config["OPENAPI_SWAGGER_UI_URL"] = "http://cdn.jsdelivr.net/npm/swagger-ui-dist/"

    api = Api(app)   
    api.register_blueprint(ReportBlueprint)

    return app