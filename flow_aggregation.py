# Flow data Aggregation Service

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import pandas as pd

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
db = SQLAlchemy(app)


class Flows(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    src_app = db.Column(db.String(80), nullable=False)
    dest_app = db.Column(db.String(80), nullable=False)
    vpc_id = db.Column(db.String(80), nullable=False)
    bytes_tx = db.Column(db.Integer)
    bytes_rx = db.Column(db.Integer)
    hour = db.Column(db.Integer)

    def __repr__(self):
        return f"{self.src_app} - {self.dest_app} - {self.vpc_id} - {self.bytes_tx} - {self.bytes_rx} - {self.hour}"


db.create_all()
db.session.commit()


@app.route('/')
def index():
    return 'Hello!'


@app.route('/flows', methods=['GET'])
def read_flows():
    hour = request.args.get('hour')
    output = []
    if hour is None:
        flows = Flows.query.all()
    else:
        flows = Flows.query.filter_by(hour=hour).all()
    for flow in flows:
        # print(flow)
        flow_data = {
            'src_app': flow.src_app,
            'dest_app': flow.dest_app,
            'vpc_id': flow.vpc_id,
            'bytes_tx': flow.bytes_tx,
            'bytes_rx': flow.bytes_rx,
            'hour': flow.hour
        }
        output.append(flow_data)
    return jsonify(output)


@app.route('/flows', methods=['POST'])
def write_flows():
    input_flow_data = request.json
    input_flow_list = []

    # Convert write input flow data into a list
    for info in input_flow_data:
        flow_data = {
            "src_app": info["src_app"],
            "dest_app": info["dest_app"],
            "vpc_id": info["vpc_id"],
            "bytes_tx": info["bytes_tx"],
            "bytes_rx": info["bytes_rx"],
            "hour": info["hour"]
        }
        input_flow_list.append(flow_data)

    # Aggregate the input flow data by src_app, dest_app, vpc_id and hour
    df = pd.DataFrame(input_flow_list)
    aggregated_df = df.groupby(['src_app', 'dest_app', 'vpc_id', 'hour']).sum()
    formatted_data = aggregated_df.reset_index()
    aggregated_flow_list = formatted_data.to_dict('records')

    # Iterate the aggregated flow list and add to the database
    for flow in aggregated_flow_list:
        flow_db_entry = Flows.query.filter_by(
            src_app=flow['src_app'], dest_app=flow['dest_app'],
            vpc_id=flow['vpc_id'], hour=flow['hour']).first()
        if flow_db_entry is None:
            flow_new_entry = Flows(src_app=flow["src_app"], dest_app=flow["dest_app"], vpc_id=flow["vpc_id"],
                                   bytes_tx=flow["bytes_tx"], bytes_rx=flow["bytes_rx"], hour=flow["hour"])
            db.session.add(flow_new_entry)
            db.session.commit()
        else:
            flow_db_entry.bytes_tx = flow_db_entry.bytes_tx + flow['bytes_tx']
            flow_db_entry.bytes_rx = flow_db_entry.bytes_rx + flow['bytes_rx']
            db.session.commit()

    return jsonify(aggregated_flow_list)