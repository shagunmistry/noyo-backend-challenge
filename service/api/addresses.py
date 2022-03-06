import json
import logging

from datetime import datetime, date

from flask import abort, jsonify
from service.api.persons import PersonResultSchema
from webargs.flaskparser import use_args

from marshmallow import Schema, fields

from service.server import app, db
from service.models import AddressSegment
from service.models import Person


class GetAddressQueryArgsSchema(Schema):
    date = fields.Date(required=False, missing=datetime.utcnow().date())


class AddressSchema(Schema):
    class Meta:
        ordered = True

    street_one = fields.Str(required=True, max=128)
    street_two = fields.Str(max=128)
    city = fields.Str(required=True, max=128)
    state = fields.Str(required=True, max=2)
    zip_code = fields.Str(required=True, max=10)

    start_date = fields.Date(required=True)
    end_date = fields.Date(required=False)



def create_new_address_segment(payload, person_id):
    address_segment = AddressSegment(
            street_one=payload.get("street_one"),
            street_two=payload.get("street_two"),
            city=payload.get("city"),
            state=payload.get("state"),
            zip_code=payload.get("zip_code"),
            start_date=payload.get("start_date"),
            person_id=person_id,
        )

    db.session.add(address_segment)
    db.session.commit()
    db.session.refresh(address_segment)
    return address_segment


@app.route("/api/persons/<uuid:person_id>/address", methods=["GET"])
@use_args(GetAddressQueryArgsSchema(), location="querystring")
def get_address(args, person_id):
    person = Person.query.get(person_id)
    if person is None:
        abort(404, description="person does not exist")
    elif len(person.address_segments) == 0:
        abort(404, description="person does not have an address, please create one")

    address_segment = person.address_segments[-1]
    return jsonify(AddressSchema().dump(address_segment))


@app.route("/api/persons/<uuid:person_id>/address", methods=["PUT"])
@use_args(AddressSchema())
def create_address(payload, person_id):
    person = Person.query.get(person_id)
    if person is None:
        abort(404, description="person does not exist")
    # If there are no AddressSegment records present for the person, we can go
    # ahead and create with no additional logic.
    elif len(person.address_segments) == 0:
        address_segment = create_new_address_segment(payload, person_id)

    elif len(person.address_segments) >= 1:
        latest_address = person.address_segments[-1]
        latest_address_schema = AddressSchema().dump(latest_address)
        latest_address_start_date = latest_address_schema['start_date']
        latest_address_start_date = datetime.strptime(latest_address_start_date, '%Y-%m-%d').date()
        if payload.get('start_date') < latest_address_start_date:
            raise Exception('New Address Start Date needs to be greater than most recent address')
        else:
            today = date.today()
            latest_address.end_date = today.strftime("%Y-%m-%d")
            db.session.commit()
            address_segment = create_new_address_segment(payload, person_id)
            return jsonify(AddressSchema().dump(latest_address))
    else:
        raise NotImplementedError()

    return jsonify(AddressSchema().dump(address_segment))
