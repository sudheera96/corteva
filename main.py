from flask import Flask
from flask_restx import Api
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine,func
from flask import request, jsonify
from flask_swagger_ui import get_swaggerui_blueprint
from flask_restx import Resource, fields
from data_model import WeatherData, WeatherStats
from ingestion import WeatherDataIngestor
from data_analysis import WeatherAnalyzer

app = Flask(__name__)
api = Api(app,title='My Weather Flask Application',default="Corteva", default_label="For Both Weathear Data and Weather Stats click on DropDown")



@api.route('/weather/<int:page>/<int:per_page>/<string:year>/<string:station_id>')
class WeathersData(Resource):
    @api.doc(
        description='Get weather data',
        params={
            'page': 'Page number',
            'per_page': 'Number of items per page',
            'station_id': 'Filter by Station_id',
            'year': 'Filter by year'
        },
        responses={
            200: 'Success',
            400: 'Bad Request',
            500: 'Internal Server Error'
        }
    )
    def to_dict(self, obj):
        """
        Helper function to convert ORM object to a dictionary
        """
        if not obj:
            return None
        return {c.name: str(getattr(obj, c.name)) for c in obj.__table__.columns}

    def get(self, page, per_page, year, station_id):
        page = int(request.args.get('page', page))
        per_page = int(request.args.get('per_page', per_page))
        year_filter = request.args.get('year', year)
        station_id_parts = request.args.get('station_id', station_id).split('C')
        if len(station_id_parts) != 2:
            return jsonify({'error': 'Invalid station ID'})

        station_id = 'USC' + station_id_parts[1]

        engine = create_engine('sqlite:///weatherus.db')
        Session = scoped_session(sessionmaker(bind=engine))
        session = Session()
        query = session.query(WeatherData)
        if year_filter:
            query = query.filter(func.strftime('%Y', WeatherData.date) == year_filter)
        if station_id:
            query = query.filter(WeatherData.station_id == station_id)
        weather_data = query.limit(per_page).offset((page - 1) * per_page).all()
        return jsonify({
            'data': [self.to_dict(row) for row in weather_data],
            'page': page,
            'per_page': per_page,
        })

    # @api.doc(responses={403: 'Not Authorized'})
    # def (self, page,per_page):
    #     api.abort(403)


@api.route('/weather/stats/<year>/<station_id>')
class WeathersStats(Resource):
    @api.doc(
        description='Get weather data statistics',
        params={
            'year': 'Filter by year',
            'station_id': 'Filter by station ID'
        },
        responses={
            200: 'Success',
            400: 'Bad Request',
            500: 'Internal Server Error'
        }
    )
    def to_dict(self, obj):
        """
        Helper function to convert ORM object to a dictionary
        """
        if not obj:
            return None
        return {c.name: str(getattr(obj, c.name)) for c in obj.__table__.columns}

    def get(self, year, station_id):
        station_id_parts = request.args.get('station_id', station_id).split('C')
        if len(station_id_parts) != 2:
            return jsonify({'error': 'Invalid station ID'})

        station_id = 'USC' + station_id_parts[1]

        year = int(request.args.get('year', year))
        engine = create_engine('sqlite:///weatherus.db')
        Session = scoped_session(sessionmaker(bind=engine))
        session = Session()
        # Apply filters if provided
        query = session.query(WeatherStats)
        if year:
            query = query.filter(WeatherStats.year == year)
        if station_id:
            query = query.filter(WeatherStats.station_id == station_id)
        weather_stats = query.all()
        return jsonify({
            'data': [self.to_dict(row) for row in weather_stats],
        })


# Create a Swagger UI endpoint
SWAGGER_URL = '/api/weather'
API_URL = '/api/weather/stats'
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "My Flask Application"

    }
)
app.register_blueprint(swaggerui_blueprint)

## Execute ingestor and analyser first then run app
if __name__ == '__main__':
    ingestor = WeatherDataIngestor('sqlite:///weatherus.db')
    ingestor.ingest('wx_data')
    analyzer = WeatherAnalyzer('sqlite:///weatherus.db')
    analyzer.calculate_statistics()
    app.run(debug=True)
