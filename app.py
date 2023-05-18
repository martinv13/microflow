from sanic import Sanic, json
from sanic.blueprints import Blueprint
import pkg_resources


def make_sanic_app(instance):

    app = Sanic("microflow_monitor")

    static_folder = pkg_resources.resource_filename(__name__, "static")
    app.static("/", static_folder + "/index.html")
    app.static("/", static_folder)

    api = Blueprint("api", url_prefix="/api")

    @api.get("/managers")
    def get_managers(request):
        return json({"hello": "world"})

    app.blueprint(api)

    return app
