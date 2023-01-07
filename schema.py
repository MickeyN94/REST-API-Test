from marshmallow import Schema, fields

class DateSchema(Schema):
    date = fields.Str(required=True)