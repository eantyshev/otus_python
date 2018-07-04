#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
import json
from datetime import datetime
import logging
import hashlib
import uuid
from optparse import OptionParser
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

import scoring

SALT = "Otus"
ADMIN_LOGIN = "admin"
ADMIN_SALT = "42"
OK = 200
BAD_REQUEST = 400
FORBIDDEN = 403
NOT_FOUND = 404
INVALID_REQUEST = 422
INTERNAL_ERROR = 500
ERRORS = {
    BAD_REQUEST: "Bad Request",
    FORBIDDEN: "Forbidden",
    NOT_FOUND: "Not Found",
    INVALID_REQUEST: "Invalid Request",
    INTERNAL_ERROR: "Internal Server Error",
}
UNKNOWN = 0
MALE = 1
FEMALE = 2
GENDERS = {
    UNKNOWN: "unknown",
    MALE: "male",
    FEMALE: "female",
}


class ValidationError(Exception):
    pass


class Field(object):
    __metaclass__ = ABCMeta

    def __init__(self, required=True, nullable=False):
        self.required = required
        self.nullable = nullable
        self.label = None

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__.get(self.label)

    def __set__(self, instance, value):
        instance.__dict__[self.label] = value

    @abstractmethod
    def validate(cls, value):
        pass


class FieldOwner(type):
    def __new__(meta, name, bases, attrs):
        # find all descriptors, auto-set their labels
        fields = []
        for n, v in attrs.items():
            if isinstance(v, Field):
                v.label = n
                fields.append(n)
        attrs['fields'] = fields
        return super(FieldOwner, meta).__new__(meta, name, bases, attrs)


class BaseRequest(object):
    __metaclass__ = FieldOwner

    def __init__(self, arguments):
        for f in self.fields:
            if f in arguments:
                setattr(self, f, arguments[f])

    def validate_fields(self):
        cls = self.__class__
        for field in self.fields:
            d = getattr(cls, field)
            if field not in self.__dict__:
                if d.required:
                    raise ValidationError(
                        "Required field %s is not defined!" % field)
                continue
            value = self.__dict__[field]
            if not d.nullable and not value:
                raise ValidationError("Non-nullable field %s is %r" %
                                      (field, value))
            try:
                d.validate(value)
            except ValidationError as exc:
                raise ValidationError("Field %s (type %s) invalid: %s (%r)" %
                                      (
                                          field,
                                          d.__class__.__name__,
                                          exc.message,
                                          value
                                      )
                                      )


class CharField(Field):
    @classmethod
    def validate(cls, value):
        if not isinstance(value, (str, unicode)):
            raise ValidationError("Not a str/unicode")


class ArgumentsField(Field):
    @classmethod
    def validate(cls, value):
        if not isinstance(value, dict):
            raise ValidationError("Is not a dict")


class EmailField(CharField):
    @classmethod
    def validate(cls, value):
        super(EmailField, cls).validate(value)
        if "@" not in value:
            raise ValidationError("email should contain @")


class PhoneField(Field):
    @classmethod
    def validate(cls, value):
        if isinstance(value, int):
            value = str(value)
        # TODO: why not check if phone number has only digits?
        if not (len(value) == 11 and value.startswith("7")):
            raise ValidationError("Phone should be of 11 symbols long and "
                                  "to start with '7'")


class DateField(Field):
    @classmethod
    def validate(cls, value):
        try:
            return datetime.strptime(value, "%d.%m.%Y")
        except (TypeError, ValueError) as e:
            raise ValidationError("Not a valid date")


class BirthDayField(DateField):
    @classmethod
    def validate(cls, value):
        dt = super(BirthDayField, cls).validate(value)
        now = datetime.now()
        if not (dt < now and now.year <= dt.year + 70):
            raise ValidationError("Valid age is between 0 and 70 years")


class GenderField(Field):
    @classmethod
    def validate(cls, value):
        if value not in GENDERS.keys():
            raise ValidationError("Gender should be in %r" % GENDERS.keys())


class ClientIDsField(Field):
    @classmethod
    def validate(cls, ids):
        if (not isinstance(ids, list) or
                not all(isinstance(i, int) for i in ids)):
            raise ValidationError("Client IDs should be list of ints")


class ClientsInterestsRequest(BaseRequest):
    client_ids = ClientIDsField(required=True)
    date = DateField(required=False, nullable=True)

    def _fill_context(self, ctx):
        ctx['nclients'] = len(self.client_ids)

    def get_result(self, ctx, store, is_admin=False):
        self._fill_context(ctx)
        return {clid: scoring.get_interests(store, clid)
                for clid in self.client_ids}


class OnlineScoreRequest(BaseRequest):
    first_name = CharField(required=False, nullable=True)
    last_name = CharField(required=False, nullable=True)
    email = EmailField(required=False, nullable=True)
    phone = PhoneField(required=False, nullable=True)
    birthday = BirthDayField(required=False, nullable=True)
    gender = GenderField(required=False, nullable=True)

    def validate_fields(self):
        super(OnlineScoreRequest, self).validate_fields()
        if not ((self.first_name and self.last_name) or
                (self.email and self.phone) or
                (self.birthday and self.gender is not None)):
            raise ValidationError("At least one of the pairs should be defined: "
                                  "first/last name, email/phone, birthday/gender")

    def _fill_context(self, ctx):
        ctx['has'] = [f for f in self.fields if getattr(self, f) is not None]

    def get_result(self, ctx, store, is_admin=False):
        self._fill_context(ctx)
        if is_admin:
            return {"score": 42}
        return {
            "score": scoring.get_score(
                store,
                self.phone,
                self.email,
                self.birthday,
                self.gender,
                self.first_name,
                self.last_name
            )
        }


class MethodRequest(BaseRequest):
    account = CharField(required=False, nullable=True)
    login = CharField(required=True, nullable=True)
    token = CharField(required=True, nullable=True)
    arguments = ArgumentsField(required=True, nullable=True)
    method = CharField(required=True, nullable=False)

    @property
    def is_admin(self):
        return self.login == ADMIN_LOGIN


def check_auth(request):
    if request.is_admin:
        digest = hashlib.sha512(datetime.now().strftime("%Y%m%d%H") + ADMIN_SALT).hexdigest()
    else:
        digest = hashlib.sha512(request.account + request.login + SALT).hexdigest()
    if digest == request.token:
        return True
    return False


def method_handler(request, ctx, store):
    request_map = {
        'online_score': OnlineScoreRequest,
        'clients_interests': ClientsInterestsRequest,
    }
    method_request = MethodRequest(request['body'])
    try:
        method_request.validate_fields()
    except ValidationError, e:
        return e.message, INVALID_REQUEST

    if not check_auth(method_request):
        return None, FORBIDDEN

    if method_request.method not in request_map:
        err = "Unknown method %s, choose any of: %s" % (method_request.method,
                                                        request_map.keys())
        return err, INVALID_REQUEST

    req = request_map[method_request.method](method_request.arguments)
    try:
        req.validate_fields()
    except ValidationError, e:
        return e.message, INVALID_REQUEST

    result = req.get_result(ctx, store, is_admin=method_request.is_admin)

    return result, OK


class MainHTTPHandler(BaseHTTPRequestHandler):
    router = {
        "method": method_handler
    }
    store = None

    def get_request_id(self, headers):
        return headers.get('HTTP_X_REQUEST_ID', uuid.uuid4().hex)

    def do_POST(self):
        response, code = {}, OK
        context = {"request_id": self.get_request_id(self.headers)}
        request = None
        try:
            data_string = self.rfile.read(int(self.headers['Content-Length']))
            request = json.loads(data_string)
        except:
            code = BAD_REQUEST

        if request:
            path = self.path.strip("/")
            logging.info("%s: %s %s" % (self.path, data_string, context["request_id"]))
            if path in self.router:
                try:
                    response, code = self.router[path]({"body": request, "headers": self.headers}, context, self.store)
                except Exception, e:
                    logging.exception("Unexpected error: %s" % e)
                    code = INTERNAL_ERROR
            else:
                code = NOT_FOUND

        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if code not in ERRORS:
            r = {"response": response, "code": code}
        else:
            r = {"error": response or ERRORS.get(code, "Unknown Error"), "code": code}
        context.update(r)
        logging.info(context)
        self.wfile.write(json.dumps(r))
        return


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-p", "--port", action="store", type=int, default=8080)
    op.add_option("-l", "--log", action="store", default=None)
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    server = HTTPServer(("localhost", opts.port), MainHTTPHandler)
    logging.info("Starting server at %s" % opts.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
